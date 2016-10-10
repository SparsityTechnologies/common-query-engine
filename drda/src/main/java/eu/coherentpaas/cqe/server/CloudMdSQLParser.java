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

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

import cql.CQL;
import cql.Parser;

public class CloudMdSQLParser {

	static {
		String path = ".";
		try {
			String os = System.getProperty("os.name").toLowerCase();
			String lib = "";
			if (os.contains("linux")) {
				lib = "libjcql.so";
			} else if (os.contains("mac")) {
				lib = "libjcql.dylib";
			} 
			else if (os.contains("win")) {
				lib = "libjcql.dll";
			}else {
				throw new RuntimeException(os + " is not supported");
			}
			InputStream is = Thread.currentThread().getContextClassLoader()
					.getResourceAsStream("bin/" + lib);
			FileOutputStream fos = null;
			try {
				byte[] buffer = new byte[2000];
				File tmp = new File(lib);
				fos = new FileOutputStream(tmp);
				int count = is.read(buffer);
				while (count > 0) {
					fos.write(buffer, 0, count);
					count = is.read(buffer);
				}
				fos.flush();
				path = tmp.getAbsolutePath();
			} finally {
				if (is != null) {
					is.close();
				}
				if (fos != null) {
					fos.close();
				}
			}
		} catch (Exception e) {
		}

		System.load(path);
	}

	public CloudMdSQLExpr parseQueryLanguageExpr(String expr) throws Exception {
		Parser parser = null;
		CQL cql = null;
		CloudMdSQLExpr node = null;
		try {
			cql = new CQL();
			// add capabilities
			Map<String, JsonNode> capabilities = CloudMdSQLManager
					.getInstance().getCapabilities();
			Set<String> datastores = capabilities.keySet();
			for (String ds : datastores) {
				JsonNode capability = capabilities.get(ds);
				if (capability != null) {
					cql.addDatastoreCapabilities(ds, capability.toString());
				} else {
					cql.addDatastoreCapabilities(ds, null);
				}

			}

			parser = cql.createParser();

			String result = parser.parse(expr);
			ObjectMapper mapper = new ObjectMapper();
			TableExprDeserializer deserializer = new TableExprDeserializer(
					TableExpr.class);
			SimpleModule module = new SimpleModule(
					"PolymorphicQueryPlanDeserializerModule",
					Version.unknownVersion());
			module.addDeserializer(TableExpr.class, deserializer);
			mapper.registerModule(module);
			System.out.println(result);
			node = mapper.readValue(result, CloudMdSQLExpr.class);
		} finally {
			if (cql != null) {
				cql.close();
			}
			if (parser != null) {
				parser.close();
			}
		}
		return node;
	}

	public CloudMdSQLExpr parseQueryPlanExpr(String expr) throws Exception {
		CloudMdSQLExpr node = null;
		ObjectMapper mapper = new ObjectMapper();
		if (!expr.startsWith("{")) {
			expr = "{" + expr + "}";
		}
		TableExprDeserializer deserializer = new TableExprDeserializer(
				TableExpr.class);
		SimpleModule module = new SimpleModule(
				"PolymorphicQueryPlanDeserializerModule",
				Version.unknownVersion());
		module.addDeserializer(TableExpr.class, deserializer);
		mapper.registerModule(module);
		node = mapper.readValue(expr, CloudMdSQLExpr.class);
		return node;
	}

	public static void main(String[] args) throws Exception {
		String t1 = "T1 (a int, b string)@DB1 = (SELECT a, b FROM tb1 WHERE id > 10) \n";
		String query = t1 + "SELECT T1.a FROM T1";
		CloudMdSQLParser parser = new CloudMdSQLParser();
		parser.parseQueryLanguageExpr(query);
	}
}
