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
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigInteger;
import java.sql.Blob;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import eu.coherentpaas.cqe.DirectResultSet;
import eu.coherentpaas.cqe.QueryPlan.Parameter;
import eu.coherentpaas.cqe.Type;
import eu.coherentpaas.cqe.datastore.Statement;

public class WrapperFunction {

	private static Object readObject(String tableName, Long transactionId, Object... args) throws Exception {
		return readObjectAt(tableName, 0, transactionId, args);
	}

	private static Object readObjectAt(String tableName, Integer position, Long transactionId, Object... args)
			throws Exception {
		CloudMdSQLManager manager = CloudMdSQLManager.getInstance();
		QueryContext ctx = manager.getQueryContext(transactionId);
		Statement stmt = ctx.getTableStatement(tableName);
		Parameter[] params = ctx.getTableParameters(tableName);
		prepareStmt(params, stmt, args);

		eu.coherentpaas.cqe.ResultSet rs = stmt.execute(ctx.getTransactionContext());
		Object[][] objects = rs.next();
		if (objects == null || objects.length == 0) {
			return null;
		} else {
			return objects[0][position];
		}
	}

	public static String readString(String tableName, Long transactionId, Object... args) throws Exception {
		return (String) readObject(tableName, transactionId, args);
	}

	public static String readStringAt(String tableName, Long transactionId, Integer position, Object... args)
			throws Exception {
		return (String) readObjectAt(tableName, position, transactionId, args);
	}

	public static Integer readInt(String tableName, Long transactionId, Object... args) throws Exception {
		return (Integer) readObject(tableName, transactionId, args);
	}

	public static Integer readIntAt(String tableName, Long transactionId, Integer position, Object... args)
			throws Exception {
		return (Integer) readObjectAt(tableName, position, transactionId, args);
	}

	public static Double readDouble(String tableName, Long transactionId, Object... args) throws Exception {
		Object o = readObject(tableName, transactionId, args);
		if(o instanceof Float){
			return ((Float)o).doubleValue();
		}
		return (Double) o;
	}

	public static Double readDoubleAt(String tableName, Long transactionId, Integer position, Object... args)
			throws Exception {
		Object o = readObjectAt(tableName, position, transactionId, args);
		if(o instanceof Float){
			return ((Float)o).doubleValue();
		}
		return (Double) o;
	}

	public static Float readFloat(String tableName, Long transactionId, Object... args) throws Exception {
		return (Float) readObject(tableName, transactionId, args);
	}

	public static Float readFloatAt(String tableName, Long transactionId, Integer position, Object... args)
			throws Exception {
		return (Float) readObjectAt(tableName, position, transactionId, args);
	}

	public static Date readDate(String tableName, Long transactionId, Object... args) throws Exception {
		return (Date) readObject(tableName, transactionId, args);
	}

	public static Date readDateAt(String tableName, Long transactionId, Integer position, Object... args)
			throws Exception {
		return (Date) readObject(tableName, transactionId, args);
	}

	public static byte[] readByteArray(String tableName, Long transactionId, Object... args) throws Exception {
		return (byte[]) readObject(tableName, transactionId, args);
	}

	public static Object[] readArrayAt(String tableName, Long transactionId, Integer position, Object... args)
			throws Exception {
		return (Object[]) readObjectAt(tableName, position, transactionId, args);
	}

	public static Object[] readArray(String tableName, Long transactionId, Object... args) throws Exception {
		return (Object[]) readObject(tableName, transactionId, args);
	}

	public static Map readMap(String tableName, Long transactionId, Object... args) throws Exception {
		return (Map) readObject(tableName, transactionId, args);
	}

	public static Map readMapAt(String tableName, Long transactionId, Integer position, Object... args)
			throws Exception {
		return (Map) readObjectAt(tableName, position, transactionId, args);
	}

	public static Long readLong(String tableName, Long transactionId, Object... args) throws Exception {
		return (Long) readObject(tableName, transactionId, args);
	}

	public static Long readLongAt(String tableName, Long transactionId, Integer position, Object... args)
			throws Exception {
		return (Long) readObjectAt(tableName, position, transactionId, args);
	}

	public static Boolean readBoolean(String tableName, Long transactionId, Object... args) throws Exception {
		return (Boolean) readObject(tableName, transactionId, args);
	}

	public static Boolean readBooleanAt(String tableName, Long transactionId, Integer position, Object... args)
			throws Exception {
		return (Boolean) readObjectAt(tableName, position, transactionId, args);
	}

	public static Serializable readSerializable(String tableName, Long transactionId, Object... args) throws Exception {
		return (Serializable) readObject(tableName, transactionId, args);
	}

	public static Serializable readSerializableAt(String tableName, Long transactionId, Integer position,
			Object... args) throws Exception {
		return (Serializable) readObjectAt(tableName, position, transactionId, args);
	}

	private static void prepareStmt(Parameter[] params, Statement stmt, Object... args) throws Exception {
		if (params != null) {
			int index = 0;
			for (Parameter param : params) {
				if (args != null && index < args.length) {
					Object value = args[index];
					Type type = param.getType();
					if (type == Type.STRING) {
						stmt.setString(param.getName(), (String) value);
					} else if (type == Type.INT) {
						stmt.setInt(param.getName(), (Integer) value);
					} else if (type == Type.LONG) {
						if (value instanceof Long) {
							stmt.setLong(param.getName(), (Long) value);
						} else if (value instanceof BigInteger) {
							stmt.setLong(param.getName(), ((BigInteger) value).longValue());
						}
					} else if (type == Type.TIMESTAMP) {
						stmt.setTimestamp(param.getName(), (Timestamp) value);
					} else if (type == Type.ARRAY) {
						stmt.setArray(param.getName(), (ArrayList<?>) value);
					} else if (type == Type.DICTIONARY) {
						stmt.setDictionary(param.getName(), (HashMap<?, ?>) value);
					} else if (type == Type.DOUBLE) {
						stmt.setDouble(param.getName(), (Double) value);
					} else if (type == Type.FLOAT) {
						if(value instanceof Double){
							stmt.setDouble(param.getName(), (Double) value);
						}
						else{
							stmt.setFloat(param.getName(), (Float) value);
						}
					} else if (type == Type.BINARY) {
						stmt.setBinary(param.getName(), (Blob) value);
					} else if (type == Type.DATE) {
						stmt.setDate(param.getName(), (Date) value);
					}
				}

				index++;
			}
		}
	}

	public static ResultSet readWithoutCatching(String tableName, Long transactionId, Object... args) throws Exception {
		CloudMdSQLManager manager = CloudMdSQLManager.getInstance();
		QueryContext ctx = manager.getQueryContext(transactionId);
		Statement stmt = ctx.getTableStatement(tableName);
		Parameter[] params = ctx.getTableParameters(tableName);
		prepareStmt(params, stmt, args);

		WrapperResultSet rs = new WrapperResultSet(ctx, tableName, stmt.execute(ctx.getTransactionContext()));
		return rs;

	}

	public static ResultSet read(String tableName, Long transactionId, Object... args) throws Exception {

		long time = System.currentTimeMillis();

		CloudMdSQLManager manager = CloudMdSQLManager.getInstance();
		QueryContext ctx = manager.getQueryContext(transactionId);
		ResultSet cached = null;
		try {
			cached = ctx.getCachedResultSet(tableName, args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (cached == null) {
			File file = new File(tableName + "_exec_profile.txt");
			FileWriter fw = null;
			BufferedWriter writer;
			try {
				fw = new FileWriter(file, true);
				writer = new BufferedWriter(fw);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			System.out.println("Asking for " + tableName);
			Statement stmt = ctx.getTableStatement(tableName);
			Parameter[] params = ctx.getTableParameters(tableName);
			prepareStmt(params, stmt, args);

			eu.coherentpaas.cqe.ResultSet rsWrapper = stmt.execute(ctx.getTransactionContext());

			ResultSet result = null;

			if (!ctx.requiresTableStore(tableName) && rsWrapper instanceof DirectResultSet) {
				result = ((DirectResultSet) rsWrapper).unwrap();
			} else {
				WrapperResultSet rs = new WrapperResultSet(ctx, tableName, rsWrapper);

				ctx.putResultSet(tableName, rs, args);
				result = rs;
			}
			long execTime = System.currentTimeMillis() - time;
			System.out.println("The function table " + tableName + " solved in " + execTime + " millis");
			System.out.println("The tx context is " + ctx.getTransactionContext().toString());
			try {
				writer.write(execTime + "\n");
				writer.close();
				fw.close();
			} catch (IOException e) {
				e.printStackTrace();
			}

			return result;
		} else if (cached instanceof WrapperResultSet) {
			((WrapperResultSet) cached).setUseCache(true);
		}
		return cached;
	}

}
