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

import java.sql.Blob;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.derby.impl.jdbc.EmbedConnection;

import eu.coherentpaas.cqe.CQEException;
import eu.coherentpaas.cqe.QueryPlan;
import eu.coherentpaas.cqe.ResultSet;
import eu.coherentpaas.cqe.TransformQueryPlan;
import eu.coherentpaas.cqe.Type;
import eu.coherentpaas.cqe.datastore.Parameterized;
import eu.coherentpaas.cqe.datastore.Statement;
import eu.coherentpaas.transactionmanager.client.TxnCtx;

/**
 * It represents a Derby statement to execute CloudMdSQL named table
 * expressions.
 * 
 * Example (Check T3):
 * 
 * T1( a int, b string )@DB1 = ( SELECT a, b FROM tbl WHERE id > 100 ) T2( a
 * int, c string )@DB2 = {* db.find( { id: 200 } ) *} T3( a int, b string, c
 * string) = (SELECT T1.a, T1.b, T2.c FROM T1 JOIN T2 ON T1.a = T2.a)
 * 
 * SELECT a, b, c FROM T3
 * 
 * @author rpau
 *
 */
public class EmbeddedStatement implements Statement {

	private CloudMdSQLStatement stmt;
	private PreparedStatement preparedStmt;
	private Map<String, Integer> params;
	private String sql = "";
	private QueryContext ctx;
	private Type[] signature;
	private String name;

	public EmbeddedStatement(String name, QueryContext ctx,
			CloudMdSQLStatement stmt, Map<String, Integer> params,
			Type[] signature) {
		this.stmt = stmt;
		this.params = params;
		this.ctx = ctx;
		this.signature = signature;
		this.name = name;
	}

	private List<Integer> getPositions(String parameterName) {
		List<Integer> positions = null;
		Map<String, List<Integer>> paramsMapping = ctx
				.getNestedTableParametersMapping(name);
		if (paramsMapping != null && paramsMapping.containsKey(parameterName)) {
			positions = paramsMapping.get(parameterName);
		}
		return positions;
	}

	@Override
	public void setInt(String parameterName, int value) throws CQEException {
		try {
			List<Integer> positions = getPositions(parameterName);
			if (positions != null) {
				for (Integer position : positions) {
					preparedStmt.setInt(position, value);
				}
			}

		} catch (SQLException e) {
			CQEException ex = new CQEException(null, "Error executing " + sql,
					e);
			ex.setStackTrace(e.getStackTrace());
			throw ex;
		}
	}

	@Override
	public void setString(String parameterName, String value)
			throws CQEException {
		try {

			List<Integer> positions = getPositions(parameterName);
			if (positions != null) {
				for (Integer position : positions) {
					preparedStmt.setString(position, value);
				}
			}

		} catch (SQLException e) {
			CQEException ex = new CQEException(null, "Error executing " + sql,
					e);
			ex.setStackTrace(e.getStackTrace());
			throw ex;
		}
	}

	@Override
	public void setTimestamp(String parameterName, Timestamp value)
			throws CQEException {
		try {
			List<Integer> positions = getPositions(parameterName);
			if (positions != null) {
				for (Integer position : positions) {
					preparedStmt.setTimestamp(position, value);
				}
			}
			
		} catch (SQLException e) {
			CQEException ex = new CQEException(null, "Error executing " + sql,
					e);
			ex.setStackTrace(e.getStackTrace());
			throw ex;
		}
	}

	@Override
	public void setArray(String parameterName, ArrayList<?> value)
			throws CQEException {
		try {
			List<Integer> positions = getPositions(parameterName);
			if (positions != null) {
				for (Integer position : positions) {
					preparedStmt.setObject(position, value);
				}
			}
			
			
		} catch (SQLException e) {
			CQEException ex = new CQEException(null, "Error executing " + sql,
					e);
			ex.setStackTrace(e.getStackTrace());
			throw ex;
		}

	}

	@Override
	public void setDictionary(String parameterName, HashMap<?, ?> value)
			throws CQEException {
		try {
			List<Integer> positions = getPositions(parameterName);
			if (positions != null) {
				for (Integer position : positions) {
					
					preparedStmt.setObject(position, value);
				}
			}
			
			
		} catch (SQLException e) {
			CQEException ex = new CQEException(null, "Error executing " + sql,
					e);
			ex.setStackTrace(e.getStackTrace());
			throw ex;
		}

	}

	@Override
	public void setFloat(String parameterName, Float value) throws CQEException {
		try {
			List<Integer> positions = getPositions(parameterName);
			if (positions != null) {
				for (Integer position : positions) {
					
					preparedStmt.setFloat(position, value);
				}
			}
		} catch (SQLException e) {
			CQEException ex = new CQEException(null, "Error executing " + sql,
					e);
			ex.setStackTrace(e.getStackTrace());
			throw ex;
		}

	}

	@Override
	public void setDouble(String parameterName, Double value)
			throws CQEException {
		try {
			List<Integer> positions = getPositions(parameterName);
			if (positions != null) {
				for (Integer position : positions) {
					
					preparedStmt.setDouble(position, value);
				}
			}
		} catch (SQLException e) {
			CQEException ex = new CQEException(null, "Error executing " + sql,
					e);
			ex.setStackTrace(e.getStackTrace());
			throw ex;
		}
	}

	@Override
	public void setBinary(String parameterName, Blob value) throws CQEException {
		try {
			
			List<Integer> positions = getPositions(parameterName);
			if (positions != null) {
				for (Integer position : positions) {
					
					preparedStmt.setBlob(position, value);
				}
			}
			
		} catch (SQLException e) {
			CQEException ex = new CQEException(null, "Error executing " + sql,
					e);
			ex.setStackTrace(e.getStackTrace());
			throw ex;
		}
	}

	@Override
	public ResultSet execute(TxnCtx context) throws CQEException,
			InterruptedException {

		java.sql.ResultSet rs = null;
		try {
			// setting the parameter of the CtxtId
			// preparedStmt.setLong(params.size() + 1, context.getCtxId());
			preparedStmt.execute();
			rs = preparedStmt.getResultSet();
		} catch (SQLException e) {
			CQEException ex = new CQEException(null, "Error executing ["
					+ preparedStmt + "]", e);
			ex.setStackTrace(e.getStackTrace());
			throw ex;
		}
		return new JDBCResultSet(rs, signature);

	}

	@Override
	public void close() throws CQEException {
		try {
			preparedStmt.close();
		} catch (SQLException e) {
			CQEException ex = new CQEException(null, "Error executing " + sql,
					e);
			ex.setStackTrace(e.getStackTrace());
			throw ex;
		}

	}

	@Override
	public void useNamedTable(String name, Parameterized table)
			throws CQEException {

	}

	@Override
	public void prepare(QueryPlan query) throws CQEException {

		try {

			TransformQueryPlan trans = (TransformQueryPlan) query;

			preparedStmt = new CloudMdSQLEmbedStatement(name, ctx, stmt,
					(EmbedConnection) stmt.getConnection(), trans);

		} catch (SQLException e) {
			CQEException ex = new CQEException(null, "Error executing " + sql,
					e);
			ex.setStackTrace(e.getStackTrace());
			throw ex;
		}
	}

	@Override
	public void setLong(String parameterName, long value) throws CQEException {
		try {
			
			List<Integer> positions = getPositions(parameterName);
			if (positions != null) {
				for (Integer position : positions) {
					
					preparedStmt.setLong(position, value);
				}
			}
		} catch (SQLException e) {
			CQEException ex = new CQEException(null, "Error executing " + sql,
					e);
			ex.setStackTrace(e.getStackTrace());
			throw ex;
		}
	}

	@Override
	public Type[] getSignature() throws CQEException {
		return signature;
	}

	@Override
	public void setDate(String parameterName, Date value) throws CQEException {
		try {
			List<Integer> positions = getPositions(parameterName);
			if (positions != null) {
				for (Integer position : positions) {
					preparedStmt.setDate(position, value);
				}
			}
			
		} catch (SQLException e) {
			CQEException ex = new CQEException(null, "Error executing " + sql,
					e);
			ex.setStackTrace(e.getStackTrace());
			throw ex;
		}
		
	}

}
