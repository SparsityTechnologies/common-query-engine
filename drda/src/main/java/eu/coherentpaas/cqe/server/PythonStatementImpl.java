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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.sql.Blob;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import javax.script.Bindings;
import javax.script.CompiledScript;
import javax.script.ScriptContext;
import javax.script.ScriptEngineManager;
import javax.script.SimpleBindings;

import org.python.core.PyGenerator;
import org.python.jsr223.PyScriptEngine;

import eu.coherentpaas.cqe.CQEException;
import eu.coherentpaas.cqe.NativeQueryPlan;
import eu.coherentpaas.cqe.QueryPlan;
import eu.coherentpaas.cqe.QueryPlan.Parameter;
import eu.coherentpaas.cqe.ResultSet;
import eu.coherentpaas.cqe.Type;
import eu.coherentpaas.cqe.datastore.Parameterized;
import eu.coherentpaas.cqe.datastore.PythonStatement;
import eu.coherentpaas.transactionmanager.client.TxnCtx;

public class PythonStatementImpl implements PythonStatement {

	private NativeQueryPlan nativeQueryPlan;

	private Map<String, Object> params = new HashMap<String, Object>();

	private QueryContext ctxt;

	private String name;

	private org.python.jsr223.PyScriptEngine engine = (PyScriptEngine) new ScriptEngineManager()
			.getEngineByName("python");

	private ScriptContext ctx = engine.getContext();

	private Object cloudMdSQL;

	private long time = Calendar.getInstance().getTimeInMillis();

	private CompiledScript cs;

	private Bindings bindings = new SimpleBindings();

	private Type[] signature;

	public PythonStatementImpl(String name) {
		this.name = name;

	}

	@Override
	public void setInt(String parameterName, int value) throws CQEException {
		params.put(parameterName, value);

	}

	@Override
	public void setString(String parameterName, String value) throws CQEException {
		params.put(parameterName, value);
	}

	@Override
	public void setTimestamp(String parameterName, Timestamp value) throws CQEException {
		params.put(parameterName, value);

	}

	@Override
	public void setArray(String parameterName, ArrayList<?> value) throws CQEException {
		params.put(parameterName, value);
	}

	@Override
	public void setDictionary(String parameterName, HashMap<?, ?> value) throws CQEException {
		params.put(parameterName, value);
	}

	@Override
	public void setFloat(String parameterName, Float value) throws CQEException {
		params.put(parameterName, value);
	}

	@Override
	public void setDouble(String parameterName, Double value) throws CQEException {
		params.put(parameterName, value);
	}

	@Override
	public void setBinary(String parameterName, Blob value) throws CQEException {
		params.put(parameterName, value);
	}

	@Override
	public ResultSet execute(TxnCtx context) throws CQEException, InterruptedException {

		bindings.clear();

		Parameter[] params = nativeQueryPlan.getParameters();

		bindings.put("CloudMdsQL", cloudMdSQL);
		bindings.put("ctx", context);
		if (params != null) {
			for (Parameter param : params) {
				bindings.put(param.getName(), this.params.get(param.getName()));
			}
		}
		ctx.setBindings(bindings, ctx.ENGINE_SCOPE);
		try {
			cs.eval(bindings);
		} catch (Exception e) {
			throw new CQEException(null, "Error executing the Python code", e);
		}
		ResultSet rs = new PythonResultSet(ctxt, name, (PyGenerator) engine.get("r" + time));

		return rs;
	}

	private String genCode(String code, long id) throws IOException {
		String trim = code.trim();
		if ("".equals(trim)) {
			return "r" + id + " = None";
		}
		StringReader sr = new StringReader(code);
		BufferedReader br = new BufferedReader(sr);
		String imports = "import datetime\nfrom datetime import date\n";
		String functionName = "f" + id;

		String finalCode = imports + "def " + functionName + "(): \n";
		try {
			String line = br.readLine();
			while (line != null) {
				finalCode += "    " + line + "\n";
				line = br.readLine();
			}

		} finally {
			br.close();
		}
		finalCode += "r" + id + " = " + functionName + "()";
		return finalCode;
	}

	@Override
	public void close() throws CQEException {
		// TODO Auto-generated method stub
		engine.close();

	}

	@Override
	public void useNamedTable(String name, Parameterized table) throws CQEException {
		// TODO Auto-generated method stub

	}

	@Override
	public void prepare(QueryPlan query) throws CQEException {
		signature = query.getSignature();

		if (query instanceof NativeQueryPlan) {
			this.nativeQueryPlan = (NativeQueryPlan) query;
			try {
				cs = engine.compile(genCode(nativeQueryPlan.getCode(), time));

			} catch (Exception e) {
				throw new CQEException(null, "Non native query plan for a Python statement", e);
			}

		} else {
			throw new CQEException(null, "Non native query plan for a Python statement", null);
		}
	}

	@Override
	public void setCloudMdSQL(Object cloudMdSQL) {
		this.cloudMdSQL = cloudMdSQL;
	}

	@Override
	public void setLong(String parameterName, long value) throws CQEException {
		params.put(parameterName, value);
	}

	@Override
	public Type[] getSignature() throws CQEException {
		return signature;
	}

	@Override
	public void setDate(String parameterName, Date value) throws CQEException {
		params.put(parameterName, value);
	}
}