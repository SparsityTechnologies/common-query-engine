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
package org.apache.derby.drda;

import java.sql.SQLException;

import org.apache.derby.impl.drda.DRDAConnThread;
import org.apache.derby.impl.drda.DRDAStatement;
import org.apache.derby.impl.drda.Database;
import org.apache.derby.impl.drda.NetworkServerControlImpl;
import org.apache.derby.impl.drda.Pkgnamcsn;
import org.apache.derby.impl.drda.Session;

import eu.coherentpaas.cqe.server.CloudMdSQLStatement;

public class MdSQLDRDAConnThread extends DRDAConnThread {

	MdSQLDRDAConnThread() {
		super(null, null, 0, false);
	}

	MdSQLDRDAConnThread(Session session, NetworkServerControlImpl server, long timeSlice, boolean logConnections) {
		super(session, server, timeSlice, logConnections);
	}

	public Database createDatabase(String dbname) {
		Database result = new Database(dbname) {

			@Override
			public DRDAStatement newDRDAStatement(Pkgnamcsn pkgnamcsn) throws SQLException {
				DRDAStatement stmt = getDRDAStatement(pkgnamcsn);
				if (stmt != null) {
					stmt.close();
					stmt.reset();
				} else {
					stmt = new CloudMdSQLStatement(this);
					stmt.setPkgnamcsn(pkgnamcsn);
					storeStatement(stmt);
				}
				return stmt;
			}


			@Override
			public void initializeDefaultStatement() {
				this.defaultStatement = new CloudMdSQLStatement(this);
			}			

		};

		return result;

	}
}
