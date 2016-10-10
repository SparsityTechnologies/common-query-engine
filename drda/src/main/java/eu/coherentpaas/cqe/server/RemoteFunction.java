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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class RemoteFunction {

	public static ResultSet read(Long ctxId, String home, Integer opId)
			throws Exception {
		System.out.println("Asking for " + home);
		long time = System.currentTimeMillis();
		CloudMdSQLManager manager = CloudMdSQLManager.getInstance();
		Connection conn = manager.getRemoteConnection(home);
		QueryContext ctx = manager.getQueryContext(ctxId);

		PreparedStatement stmt = conn.prepareStatement(ctx.getCode(opId));
		stmt.execute();
		long execTime = System.currentTimeMillis() - time;
		System.out.println("The function table " + home + " solved in "
				+ execTime + " millis");
		return stmt.getResultSet();
	}
}
