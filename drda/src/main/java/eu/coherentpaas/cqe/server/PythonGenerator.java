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
import java.util.List;

import eu.coherentpaas.cqe.NativeQueryPlan;
import eu.coherentpaas.cqe.QueryPlan;
import eu.coherentpaas.cqe.TransformQueryPlan;
import eu.coherentpaas.cqe.QueryPlan.Parameter;

public class PythonGenerator {

	private static final String TAB = "    ";
	public String execute(CloudMdSQLExpr expr, Long ctxtId) throws IOException {

		StringWriter writter = new StringWriter();
		BufferedWriter buffer = new BufferedWriter(writter);
		
		try {
			buffer.append("from eu.coherentpaas.cqe.server import CloudMdSQLManager");
			buffer.newLine();
			buffer.append("class CloudMdSQLContext(object):");
			buffer.newLine();
			buffer.append(TAB + "ctxt = CloudMdSQLManager.getInstance().getQueryContext("+ctxtId+")");
			buffer.newLine();
			List<TableExpr> tables = expr.getSub();

			if (tables != null) {

				for (TableExpr current : tables) {

					QueryPlan plan = current.getPlan();
					if (plan != null) {
						if (plan instanceof NativeQueryPlan
								|| plan instanceof TransformQueryPlan) {

							Parameter[] params = plan.getParameters();
							buffer.append(TAB+"def " + current.getName() + " (");
							buffer.append("self");
							if (params != null) {
								for (int i = 0; i < params.length; i++) {
									buffer.append(", ");
									buffer.append(params[i].getName());									
								}
							}
							buffer.append("):");
							buffer.newLine();
							buffer.append(TAB+TAB + "return self.ctxt.getResultSetWithoutCatching(\""
									+ current.getName()+"\"");
							if (params != null) {
								buffer.append(",");
								for (int i = 0; i < params.length; i++) {
									buffer.append(params[i].getName());
									if (i + 1 < params.length) {
										buffer.append(", ");
									}
								}
							}
							
							buffer.append(")");
							buffer.newLine();

						}
					}
				}
			}
		} finally {
			buffer.flush();
			buffer.close();
			writter.close();
		}
		return writter.getBuffer().toString();
	}
}
