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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.apache.derby.drda.Column;
import org.apache.derby.iapi.jdbc.EngineConnection;
import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.impl.sql.compile.StatementNode;
import org.eclipse.jetty.util.ConcurrentHashSet;

import eu.coherentpaas.cqe.CQEException;
import eu.coherentpaas.cqe.QueryPlan;
import eu.coherentpaas.cqe.QueryPlan.Parameter;
import eu.coherentpaas.cqe.TransformQueryPlan;
import eu.coherentpaas.cqe.Type;
import eu.coherentpaas.cqe.datastore.Connection;
import eu.coherentpaas.cqe.datastore.PythonStatement;
import eu.coherentpaas.cqe.datastore.Statement;
import eu.coherentpaas.cqe.plan.Operation;
import eu.coherentpaas.transactionmanager.client.TxnCtx;

public class QueryContext {

	private LinkedHashMap<String, ResultSet> resultSets;

	private Map<String, Column[]> columns;

	private Map<String, Statement> statements = null;

	private Map<String, Parameter[]> parameters = null;

	private Map<String, Type[]> signatures = null;

	private Map<String, Set<String>> dependencies = null;

	private Set<String> remoteFunctions = null;

	private TxnCtx transactionContext = null;

	private CloudMdSQLStatement derbyStmt = null;

	private Map<String, TableExpr> tables = null;

	private Map<String, Integer> nestedTableParameterIds = null;

	private Map<String, Map<String, List<Integer>>> nestedTableParameters = null;

	private Map<Integer, String> codes = null;

	private Map<String, TableStore> tableStore = null;
	
	private Set<String> requiresTableStore= null;

	
	// stores the correspondence between the ? position and the parameter
	private ArrayList<String> mainQueryParameters = new ArrayList<String>();

	private static ScriptEngine engine = new ScriptEngineManager().getEngineByName("python");

	private long id;

	private String code;

	private int subquerySurrogate = 0;

	public QueryContext(long id, TxnCtx transactionContext, String code) {

		resultSets = new LinkedHashMap<String, ResultSet>();

		columns = Collections.synchronizedMap(new HashMap<String, Column[]>());

		statements = Collections.synchronizedMap(new HashMap<String, Statement>());

		parameters = Collections.synchronizedMap(new HashMap<String, Parameter[]>());

		signatures = Collections.synchronizedMap(new HashMap<String, Type[]>());

		remoteFunctions = Collections.synchronizedSet(new HashSet<String>());
		
		requiresTableStore = new ConcurrentHashSet<String>();

		dependencies = Collections.synchronizedMap(new HashMap<String, Set<String>>());

		tables = Collections.synchronizedMap(new HashMap<String, TableExpr>());

		codes = Collections.synchronizedMap(new HashMap<Integer, String>());

		tableStore = Collections.synchronizedMap(new HashMap<String, TableStore>());

		nestedTableParameterIds = Collections.synchronizedMap(new HashMap<String, Integer>());

		nestedTableParameters = Collections.synchronizedMap(new HashMap<String, Map<String, List<Integer>>>());

		
		this.transactionContext = transactionContext;
		this.id = id;
		this.code = code;
	}

	public String getCode() {
		return code;
	}

	public Long getId() {
		return id;
	}

	public void addParameter(String parameter) {
		mainQueryParameters.add(parameter);
	}

	public String getParameter(int position) {
		if (position < mainQueryParameters.size()) {
			return mainQueryParameters.get(position);
		}
		return null;
	}

	public TxnCtx getTransactionContext() {
		return transactionContext;
	}

	public void setTransactionContext(TxnCtx transactionContext) {
		this.transactionContext = transactionContext;
	}

	public void setCode(Integer op, String code) {
		codes.put(op, code);
	}

	public String getCode(Integer op) {
		return codes.get(op);
	}

	public void addNamedTable(String name, Column[] columns, Parameter[] parameters) {
		this.addNamedTable(name, columns, null);
	}

	public TableExpr getTableExpr(String name) {
		return tables.get(name);
	}

	public void addNamedTable(String name, Column[] columns, Parameter[] params, Statement stmt) {
		this.columns.put(name, columns);
		this.parameters.put(name, params);
		if (stmt != null) {
			statements.put(name, stmt);
		}

	}

	public TableStore getTableStore(String name) throws Exception {
		TableStore tt = tableStore.get(name);
		if (tt == null) {
			tt = new TableStoreInMemoryImpl();
			tableStore.put(name, tt);
		}
		return tt;
	}

	public Parameter[] getTableParameters(String name) {
		return parameters.get(name);
	}

	public Statement getTableStatement(String name) {
		return statements.get(name);
	}

	public void putResultSet(String name, WrapperResultSet rs, Object... args) {
		if (resultSets.size() > 10) {
			String auxKey = resultSets.keySet().iterator().next();
			resultSets.remove(auxKey);
		}
		String key = name;
		if (args != null && args.length > 0) {
			key += Arrays.deepHashCode(args);
		}

		resultSets.put(key, rs);
		rs.setId(key);
	}

	public void removeResultSet(String id) {
		resultSets.remove(id);
	}

	public ResultSet getResultSet(String name, Object... args) throws Exception {
		ResultSet rs = null;
		if (args == null || args.length == 0) {
			rs = resultSets.get(name);
		}
		if (rs == null) {
			rs = WrapperFunction.read(name, getId(), args);
		}
		return rs;
	}

	public ResultSet getResultSetWithoutCatching(String name, Object... args) throws Exception {

		return WrapperFunction.readWithoutCatching(name, getId(), args);
	}

	public ResultSet getCachedResultSet(String name, Object... args) throws Exception {

		String key = name;
		if (args != null && args.length > 0) {
			key += Arrays.deepHashCode(args);
		}

		return resultSets.get(key);
	}

	public ResultSet getResultSet(String name) throws Exception {
		ResultSet rs = getResultSet(name, new Object[0]);
		return rs;
	}

	private void dropFunctions(EngineConnection con) throws SQLException {
		resultSets.clear();
		Set<String> functions = statements.keySet();
		for (String function : functions) {
			dropFunction(function, con);
		}
		if (remoteFunctions != null) {
			for (String remote : remoteFunctions) {
				dropFunction(remote, con);
			}
			remoteFunctions.clear();
		}
		statements.clear();
	}

	private void dropFunction(String table, EngineConnection conn) throws SQLException {

		TableExpr tableDef = tables.get(table);
		QueryPlan qp = tableDef.getPlan();
		Type[] signature = qp.getSignature();

		EngineConnection con = null;
		if (conn == null) {
			con = getCloudMdSQLStatement().getConnection();
		} else {
			con = conn;
		}
		if (!qp.isScalar() || (qp.isScalar() && signature.length == 1)) {

			java.sql.Statement functionStmt = con.createStatement();
			String namespace = "CTXT" + getId();
			String name = getCloudMdSQLStatement().getLanguageConnectionContext().getCurrentSchemaName() + "."
					+ namespace + "_" + table.toUpperCase();

			functionStmt.executeUpdate("DROP FUNCTION " + name);
			functionStmt.close();

		} else {
			for (int i = 0; i < signature.length; i++) {

				java.sql.Statement functionStmt = con.createStatement();
				String namespace = "CTXT" + getId();
				String name = getCloudMdSQLStatement().getLanguageConnectionContext().getCurrentSchemaName() + "."
						+ namespace + "_" + table.toUpperCase() + "_" + i;

				functionStmt.executeUpdate("DROP FUNCTION " + name);
				functionStmt.close();
			}
		}

	}

	public void close(EngineConnection con) {
		try {
			dropFunctions(con);
		} catch (Exception e) {

		}
	}

	public void close() throws SQLException { // destroy the derby functions
		close(null);
	}

	public void removeNamedTable(String name) throws SQLException {
		ResultSet rs = resultSets.remove(name);
		if (rs != null) {
			rs.close();
		}
	}

	public String getColumnName(String table, int index) {
		return columns.get(table)[index].getName();
	}

	public int getColumnIndex(String table, String column) {
		Column[] tableColumns = columns.get(table);
		int result = -1;
		if (tableColumns != null) {
			boolean found = false;
			for (int i = 0; i < tableColumns.length && !found; i++) {
				found = tableColumns[i].getName().equals(column);
				if (found) {
					result = i;
				}
			}
		}
		return result;
	}

	public Map<String, Integer> getColumnIndexes(String table) {
		Map<String, Integer> result = new LinkedHashMap<String, Integer>();
		Column[] tableColumns = columns.get(table);
		if (tableColumns != null) {
			for (int i = 0; i < tableColumns.length; i++) {
				result.put(tableColumns[i].getName(), i);
			}
		}
		return result;
	}

	public void setCloudMdSQLStatement(CloudMdSQLStatement derbyStmt) {
		this.derbyStmt = derbyStmt;
	}

	public CloudMdSQLStatement getCloudMdSQLStatement() {
		return derbyStmt;
	}

	public boolean isNestedTable(String name) {
		return nestedTableParameterIds.containsKey(name);
	}

	public Integer createParameterId(String name, String param) {
		Integer i = nestedTableParameterIds.get(name);
		i = i + 1;
		nestedTableParameterIds.put(name, i);
		Map<String, List<Integer>> params = nestedTableParameters.get(name);
		if (params == null) {
			params = new HashMap<String, List<Integer>>();
		}
		List<Integer> positions = params.get(param);
		if (positions == null) {
			positions = new LinkedList<Integer>();
		}
		positions.add(i);
		params.put(param, positions);
		nestedTableParameters.put(name, params);

		return i;
	}

	public Map<String, List<Integer>> getNestedTableParametersMapping(String nestedTableName) {
		return nestedTableParameters.get(nestedTableName);
	}

	public StatementNode compileToSQL(CloudMdSQLExpr expr, CompilerContext cc) throws Exception {

		PythonGenerator gen = new PythonGenerator();
		String pythonClass = gen.execute(expr, getId());
		RemoteCallFunctionTablesGenerator rcfg = new RemoteCallFunctionTablesGenerator(this);
		List<TableExpr> tables = expr.getSub();
		if (tables != null) {
			for (TableExpr current : tables) {

				this.tables.put(current.getName(), current);
				QueryPlan plan = current.getPlan();

				if (plan != null) {

					String ds = plan.getDatastore();
					Type[] signature = plan.getSignature();

					Set<String> dependencies = new HashSet<String>();

					this.dependencies.put(current.getName(), dependencies);
					if (signature == null || signature.length == 0) {
						signature = new Type[1];
						signature[0] = Type.INT;
					}
					Column[] columns = new Column[signature.length];
					for (int i = 0; i < signature.length; i++) {
						columns[i] = new Column(Integer.toString(i), signature[i]);
					}
					Statement stmt = null;

					// dependencies resolution

					String[] references = plan.getReferences();
					if (references != null) {
						for (String reference : references) {
							dependencies.add(reference);
						}
					}

					if (ds.trim().equals("python")) {

						stmt = new PythonStatementImpl(current.getName());
						PythonStatement pyStmt = (PythonStatement) stmt;
						pyStmt.setCloudMdSQL(genCode(pythonClass));
					} else if (ds != null && !ds.trim().equals("")) {

						Connection con = CloudMdSQLManager.getInstance().getConnection(ds, transactionContext.getId());
						stmt = con.createStatement();
						if (stmt instanceof PythonStatement) {
							PythonStatement pyStmt = (PythonStatement) stmt;
							pyStmt.setCloudMdSQL(genCode(pythonClass));
						}

					} else {

						if (plan instanceof TransformQueryPlan) {
							Parameter[] params = plan.getParameters();
							Map<String, Integer> parameters = new HashMap<String, Integer>();
							if (params != null) {
								for (int i = 0; i < params.length; i++) {
									parameters.put(params[i].getName(), i + 1);
								}
							}
							nestedTableParameterIds.put(current.getName(), new Integer(0));
							stmt = new EmbeddedStatement(current.getName(), this, derbyStmt, parameters,
									plan.getSignature());

						} else {

							stmt = new PythonStatementImpl(current.getName());
							PythonStatement pyStmt = (PythonStatement) stmt;
							pyStmt.setCloudMdSQL(genCode(pythonClass));
						}

					}

					Parameter[] params = plan.getParameters();

					String code = generateDerbyFunction("CTXT" + getId(), plan.isScalar(), current, signature);

					String[] functions = code.split(";");
					for (int i = 0; i < functions.length; i++) {
						try {
							EngineConnection con = getCloudMdSQLStatement().getConnection();

							java.sql.Statement functionStmt = con.createStatement();
							functionStmt.execute(functions[i]);
							functionStmt.close();

						} catch (Exception e) {
							throw new CQEException(null, "Error creating the function", e);
						}
					}

					addNamedTable(current.getName(), columns, params, stmt);
					if (!CloudMdSQLManager.getInstance().isStandalone()) {
						if (plan.getDatastore().equals("") && plan instanceof TransformQueryPlan) {

							((TransformQueryPlan) plan).getPlan().accept(rcfg);
						}
					}
				}
			}

			if (!CloudMdSQLManager.getInstance().isStandalone()) {

				Operation op = expr.getPlan();

				op.accept(rcfg);

			}

			// the function declarations are created. So, embedded statements
			// can compile
			for (TableExpr current : tables) {
				Statement stmt = statements.get(current.getName());
				stmt.prepare(current.getPlan());

			}

			// tables need to be ordered from those who has less dependencies to
			// more
			TreeSet<Dependency> dependencies = new TreeSet<Dependency>();
			for (TableExpr current : tables) {
				Set<String> deps = getTransitiveDependencies(current.getName());
				dependencies.add(new Dependency(current.getName(), deps));

			}
			// now, let's use useNamedTable
			for (Dependency dep : dependencies) {
				Statement stmt = getTableStatement(dep.getTable());
				for (String item : dep.getDependencies()) {
					requiresTableStore.add(item);
					stmt.useNamedTable(item, getTableStatement(item));
				}
			}
			// then the root plan can be prepared.
			return compileToSQL(expr.getPlan(), cc);

		}

		return null;
	}
	
	public void setRequiresTableStore(String table){
		requiresTableStore.add(table);
	}
	
	public boolean requiresTableStore(String table){
		return requiresTableStore.contains(table);
	}

	public StatementNode compileToSQL(Operation plan, CompilerContext cc) throws CQEException {
		return compileToSQL(null, plan, cc);
	}

	public StatementNode compileToSQL(String nestedTableName, Operation plan, CompilerContext cc) throws CQEException {
		OperationToSQLVisitor visitor = null;

		if (CloudMdSQLManager.getInstance().isStandalone()) {
			visitor = new OperationToSQLVisitor(this, nestedTableName);
		}
		visitor.setCompilerContext(cc);

		plan.accept(visitor);
		cc.setParameterList(visitor.getParameterList());
		DataTypeDescriptor[] dvtypes = cc.getParameterTypes();
		if (dvtypes != null) {
			ArrayList<DataTypeDescriptor> dvDesc = visitor.getDataValueDescriptors();
			for (int i = 0; i < dvtypes.length; i++) {
				dvtypes[i] = dvDesc.get(i);
			}
		}
		return (StatementNode) plan.getVisitorResult();
	}

	public Set<String> getDependencies(String name) {
		return dependencies.get(name);
	}

	private Set<String> getTransitiveDependencies(String name) {
		Set<String> aux = dependencies.get(name);
		if (aux != null) {
			LinkedList<String> result = new LinkedList<String>(aux);
			Set<String> localDeps = dependencies.get(name);
			for (String dep : localDeps) {
				result.addAll(0, getTransitiveDependencies(dep));
			}
			return new LinkedHashSet<String>(result);
		}
		return new LinkedHashSet<String>();
	}

	protected void generateDerbyFunctionForRemoteOperation(String name, int opId, Type[] signature)
			throws CQEException {
		try {
			StringWriter writter = new StringWriter();
			BufferedWriter buffer = new BufferedWriter(writter);
			String result = "";
			String namespace = "CTXT" + getId();
			String functionName = name + "_REMOTE_" + opId;
			Column[] columns = new Column[signature.length];
			try {
				buffer.append("CREATE FUNCTION "
						+ getCloudMdSQLStatement().getLanguageConnectionContext().getCurrentSchemaName() + "."
						+ namespace.toUpperCase() + "_" + functionName.toUpperCase() + "(");
				buffer.append("ctxid BIGINT, ");
				buffer.append("name VARCHAR(50),");
				buffer.append("opId INTEGER");

				buffer.append(") RETURNS TABLE (");
				for (int i = 0; i < signature.length; i++) {
					buffer.append("R" + Integer.toString(i) + " ");
					buffer.append(signature[i].toSQLType());
					if (i + 1 < signature.length) {
						buffer.append(", ");
					}
					columns[i] = new Column("R" + Integer.toString(i), signature[i]);
				}
				String clazz = RemoteFunction.class.getName();

				buffer.append(") LANGUAGE JAVA PARAMETER STYLE DERBY_JDBC_RESULT_SET READS SQL DATA EXTERNAL NAME '"
						+ clazz + ".read'");
				buffer.flush();
				result = writter.getBuffer().toString();

			} finally {
				buffer.close();
				writter.close();
			}

			EngineConnection con = getCloudMdSQLStatement().getConnection();

			java.sql.Statement functionStmt = con.createStatement();
			functionStmt.executeUpdate(result);
			functionStmt.close();
			remoteFunctions.add(functionName);

			this.columns.put(functionName, columns);

		} catch (Exception e) {
			throw new CQEException(null, "Error creating the function table for a remote call ", e);
		}

	}

	public Type[] getSignature(String name) {
		return signatures.get(name);
	}

	private void writeFunctionTableHeader(BufferedWriter buffer, String schemaName, String namespace, String tname,
			Parameter[] params, int index) throws IOException {
		if (index > -1) {
			tname += "_" + index;
		}
		buffer.append("CREATE FUNCTION " + schemaName + "." + namespace + "_" + tname + "(");
		buffer.append("name VARCHAR(50), ");
		buffer.append("ctxid BIGINT");
		if (index > -1) {
			buffer.append(", ");
			buffer.append("scalarIndex" + " ");
			buffer.append(Type.INT.toSQLType());
		}
		// param para el nombre
		if (params != null) {
			for (int i = 0; i < params.length; i++) {
				buffer.append(", ");
				buffer.append(params[i].getName() + " ");
				buffer.append(params[i].getType().toSQLType());
			}
		}

	}

	public String getSchema() {
		return getCloudMdSQLStatement().getLanguageConnectionContext().getCurrentSchemaName();
	}

	private String generateDerbyFunction(String namespace, boolean isScalar, TableExpr current, Type[] signature)
			throws IOException {
		StringWriter writter = new StringWriter();
		BufferedWriter buffer = new BufferedWriter(writter);
		String result = "";
		QueryPlan plan = current.getPlan();
		Parameter[] params = plan.getParameters();
		String clazz = WrapperFunction.class.getName();
		signatures.put(current.getName(), signature);
		try {
			int index = -1;
			if (isScalar && signature.length > 1) {
				index = 0;
			}
			writeFunctionTableHeader(buffer,
					getCloudMdSQLStatement().getLanguageConnectionContext().getCurrentSchemaName(), namespace,
					current.getName().toUpperCase(), params, index);

			if (!isScalar) {
				buffer.append(") RETURNS TABLE (");

				for (int i = 0; i < signature.length; i++) {
					buffer.append("R" + Integer.toString(i) + " ");
					buffer.append(signature[i].toSQLType());
					if (i + 1 < signature.length) {
						buffer.append(", ");
					}
				}
				buffer.append(") LANGUAGE JAVA PARAMETER STYLE DERBY_JDBC_RESULT_SET READS SQL DATA EXTERNAL NAME '"
						+ clazz + ".read'");
			} else {
				String methodSuffix = "";
				if (signature.length > 1) {
					methodSuffix = "At";
				}
				for (int i = 0; i < signature.length; i++) {
					String typeMethodName = signature[i].toClass().getSimpleName();
					Character initial = typeMethodName.charAt(0);
					typeMethodName = Character.toUpperCase(initial) + typeMethodName.substring(1);

					if (signature[i].equals(Type.BINARY)) {
						typeMethodName += "Array";
					}

					buffer.append(") RETURNS " + signature[i].toSQLType()
							+ " PARAMETER STYLE JAVA NO SQL LANGUAGE JAVA EXTERNAL NAME '" + clazz + ".read"
							+ typeMethodName + methodSuffix + "'");
					if (i + 1 < signature.length) {
						buffer.append(";");
						writeFunctionTableHeader(buffer,
								getCloudMdSQLStatement().getLanguageConnectionContext().getCurrentSchemaName(),
								namespace, current.getName().toUpperCase(), params, i + 1);

					}
				}
			}

			buffer.flush();
			result = writter.getBuffer().toString();

		} finally {
			buffer.close();
			writter.close();
		}

		return result;
	}

	private Object genCode(String cloudMdSQLClassCode) throws CQEException {

		String finalCode = cloudMdSQLClassCode;
		finalCode += "CloudMdSQL = CloudMdSQLContext()\n";
		try {
			engine.eval(finalCode);
		} catch (ScriptException e) {
			throw new CQEException(null, "Error solving the python object ", e);
		}

		return engine.get("CloudMdSQL");
	}

	public String generateSubqueryName() {
		subquerySurrogate++;
		return "SUBQRY" + subquerySurrogate;
	}
}
