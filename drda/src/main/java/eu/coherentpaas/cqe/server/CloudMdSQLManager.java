/* Copyright 2016 Sparsity-Technologies
 
 The research leading to this code has been partially funded by the
 European Commission under FP7 programme project #611068.
 
 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at
 
     http://www.apache.org/licenses/LICENSE-2.0
 
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */
package eu.coherentpaas.cqe.server;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.derby.iapi.jdbc.EngineConnection;

import com.fasterxml.jackson.databind.JsonNode;

import eu.coherentpaas.cqe.CQEException;
import eu.coherentpaas.cqe.CQEException.Severity;
import eu.coherentpaas.cqe.datastore.Connection;
import eu.coherentpaas.cqe.datastore.DataStore;
import eu.coherentpaas.transactionmanager.client.LTMServerProxy;
import eu.coherentpaas.transactionmanager.client.TxnCtx;
import eu.coherentpaas.transactionmanager.exception.TransactionManagerException;

public class CloudMdSQLManager {

	private Map<Long, QueryContext> contexts;

	private Map<String, DataStore> datastores = null;

	private Map<String, Properties> properties = null;

	private Map<String, Map<Long, Connection>> connections = null;

	private Map<String, String> remoteHomes = null;

	private Map<String, java.sql.Connection> remoteConnections = new HashMap<String, java.sql.Connection>();

	private String database = null;

	private static CloudMdSQLManager instance = null;

	private String localHome = null;

	private LTMServerProxy txnManager;

	private static AtomicLong counter = new AtomicLong(System.currentTimeMillis());

	public LTMServerProxy getTransactionManager() {
		return txnManager;
	}

	private CloudMdSQLManager() {
		contexts = new ConcurrentHashMap<Long, QueryContext>();
		datastores = new ConcurrentHashMap<String, DataStore>();

		connections = new ConcurrentHashMap<String, Map<Long, Connection>>();
		properties = new ConcurrentHashMap<String, Properties>();
		try {
			txnManager = new LTMServerProxy();
		} catch (TransactionManagerException e) {
			throw new RuntimeException("Error starting the TM ", e);
		}
	}

	public static CloudMdSQLManager getInstance() {
		if (instance == null) {
			instance = new CloudMdSQLManager();
		}
		return instance;
	}

	public boolean isHereThePlaceToExecute(String home) {
		String url = remoteHomes.get(localHome);
		String url2 = remoteHomes.get(home);
		return url2 == null || url.equals(url2);
	}

	public void setLocalHome(String localHome) {
		this.localHome = localHome;
	}

	public String getLocalHome() {
		return localHome;
	}

	public java.sql.Connection getRemoteConnection(String home) throws Exception {
		java.sql.Connection c = remoteConnections.get(home);
		if (c == null) {
			String url = getRemoteURL(home);
			String database = getDatabase();

			String[] parts = url.split(":");

			java.sql.Connection conn = DriverManager.getConnection(
					"jdbc:derby://" + parts[0] + ":" + parts[1] + "/" + database + ";create=true;queryLang=cloudmdsqp");

			remoteConnections.put(home, conn);
			c = conn;
		}

		return c;
	}

	@Override
	public void finalize() {
		Collection<java.sql.Connection> cons = remoteConnections.values();
		for (java.sql.Connection c : cons) {
			try {
				c.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	public void addRemoteHome(String home, String host, int port) {
		if (remoteHomes == null) {
			remoteHomes = new HashMap<String, String>();
		}
		remoteHomes.put(home, host + ":" + port);
	}

	public String getRemoteURL(String home) {
		return remoteHomes.get(home);
	}

	public boolean isStandalone() {
		return remoteHomes == null;
	}

	public void setDatabase(String database) {
		this.database = database;
	}

	public String getDatabase() {
		return database;
	}

	public void addDataStore(String name, DataStore datastore, Properties properties) throws CQEException {

		datastore.start(properties);

		datastores.put(name, datastore);
		this.properties.put(name, properties);

	}

	public Map<String, JsonNode> getCapabilities() throws CQEException {
		Map<String, JsonNode> result = new HashMap<String, JsonNode>();

		Set<String> keys = datastores.keySet();
		if (keys != null) {
			for (String key : keys) {
				result.put(key, datastores.get(key).getMetaData());
			}
		}
		return result;
	}

	public synchronized Connection getConnection(String datastoreName, Long txId) throws CQEException {
		if (connections.containsKey(datastoreName) && connections.get(datastoreName).containsKey(txId)) {
			return connections.get(datastoreName).get(txId);
		}
		DataStore datastore = datastores.get(datastoreName);
		Properties properties = this.properties.get(datastoreName);
		if (properties == null) {
			throw new CQEException(CQEException.Severity.Execution,
					"The CQE can't find the properties for the " + datastoreName, null);
		}

		Map<Long, Connection> cons = connections.get(datastoreName);
		if (cons == null) {
			cons = new ConcurrentHashMap<Long, Connection>();
			connections.put(datastoreName, cons);
		}

		if (cons.size() > 50) {
			Set<Long> txIds = cons.keySet();

			Iterator<Long> it = txIds.iterator();
			while (it.hasNext()) {
				Long tid = it.next();

				try {
					TxnCtx ctx = getTransaction(tid);
					if (!ctx.isActive()) {
						Connection aux = cons.remove(tid);
						aux.close();
					}
				} catch (TransactionManagerException e) {
					throw new CQEException(Severity.Execution, "Error getting tx id = " + tid, e);
				}
			}
		}
		Connection connection = null;
		if (properties.containsKey("wrapper.user") && properties.containsKey("wrapper.password")) {
			connection = datastore.getConnection(properties.getProperty("wrapper.user"),
					properties.getProperty("wrapper.password"));
		} else {
			connection = datastore.getConnection();
		}
		cons.put(txId, connection);

		return connection;

	}

	public QueryContext getQueryContext(Long ctxtId) {
		return contexts.get(ctxtId);
	}

	public void removeQueryContext(Long ctxId, EngineConnection con) throws SQLException {
		QueryContext ctx = contexts.remove(ctxId);

		Set<String> keySet = connections.keySet();
		if (keySet != null) {
			for (String datastoreName : keySet) {
				Map<Long, Connection> cons = connections.get(datastoreName);
				TxnCtx tctx = ctx.getTransactionContext();
				if (!tctx.isActive()) {
					Connection aux = cons.remove(ctx.getTransactionContext().getId());
					try {
						if (aux != null) {
							aux.close();
						}
					} catch (CQEException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}
		if (ctx != null) {
			ctx.close(con);
		}

	}

	public void removeQueryContext(Long ctxId) throws SQLException {
		removeQueryContext(ctxId, null);
	}

	public TxnCtx startTransaction() throws TransactionManagerException {
		return txnManager.startTransaction();
	}

	public TxnCtx getTransaction(Long txId) throws TransactionManagerException {
		if (contexts != null) {
			Set<Long> ctxs = contexts.keySet();
			for (Long ctxId : ctxs) {
				QueryContext ctx = contexts.get(ctxId);
				if (ctx != null) {
					TxnCtx tctx = ctx.getTransactionContext();
					if (tctx.getId() == txId) {
						return tctx;
					}
				}
			}
		}
		TxnCtx result = startTransaction();
		result.setId(txId);
		return result;
	}

	public QueryContext buildQueryContext(TxnCtx txnCtx, CloudMdSQLStatement currentStmt, String code)
			throws Exception {

		Long id = counter.incrementAndGet();

		System.out.println("Starting the Id " + id);

		QueryContext ctx = contexts.get(counter);
		if (ctx != null) {
			contexts.remove(id);
		}
		ctx = new QueryContext(id, txnCtx, code);
		contexts.put(id, ctx);
		ctx.setCloudMdSQLStatement(currentStmt);
		return ctx;
	}

	public void shutdown() throws Exception {
		if (contexts != null) {
			Set<Long> ctxs = new HashSet<Long>(contexts.keySet());
			for (Long ctxId : ctxs) {
				QueryContext ctx = contexts.get(ctxId);
				if (ctx != null) {
					try {
						ctx.close();
					} catch (Exception e) {
						System.out.println("Error closing the context " + ctxId);
						e.printStackTrace();
					}
				}
			}
		}

		if (connections != null) {
			Set<String> keys = connections.keySet();
			for (String key : keys) {
				Collection<Connection> cons = connections.get(key).values();
				if (cons != null) {
					for (Connection con : cons) {
						try {
							con.close();
						} catch (Exception e) {
							System.out.println("Error closing the connection for " + key);
							e.printStackTrace();
						}
					}
				}
			}
		}
		txnManager.close();
	}

}
