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

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.derby.iapi.jdbc.EngineConnection;
import org.apache.derby.iapi.jdbc.EnginePreparedStatement;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.impl.drda.DRDAStatement;
import org.apache.derby.impl.drda.Database;
import org.apache.derby.impl.jdbc.EmbedConnection;

import eu.coherentpaas.engine.CPaaSQueryContext;

public class CloudMdSQLStatement extends DRDAStatement {

	private LanguageConnectionContext lcc = null;

	public CloudMdSQLStatement(Database database) {
		super(database);
	}

	public LanguageConnectionContext getLanguageConnectionContext() {
		return lcc;
	}

	public EngineConnection getConnection() {
		return database.getConnection();
	}

	public EnginePreparedStatement createStmt(String stmt) throws SQLException {

		ps = new CloudMdSQLEmbedStatement(this, (EmbedConnection) database.getConnection(), stmt, false, scrollType,
				concurType, withHoldCursor, Statement.NO_GENERATED_KEYS, null, null);

		return ps;
	}

	@Override
	public PreparedStatement prepare(String sqlStmt) throws SQLException {

		// save current prepare iso level
		int saveIsolationLevel = -1;
		boolean isolationSet = false;

		if (pkgnamcsn != null) {
			saveIsolationLevel = database.getPrepareIsolation();
			database.setPrepareIsolation(isolationLevel);
			isolationSet = true;
		}
		lcc = ((EmbedConnection) database.getConnection()).getLanguageConnection();

		parsePkgidToFindHoldability();

		if (isCallableSQL(sqlStmt)) {
			isCall = true;
			ps = (EnginePreparedStatement) database.getConnection().prepareCall(sqlStmt, scrollType, concurType,
					withHoldCursor);
			setupCallableStatementParams((CallableStatement) ps);
		} else {
			try {

				ps = createStmt(sqlStmt);

			} catch (Exception e) {
				if (e instanceof SQLException) {
					throw (SQLException) e;
				} else {
					SQLException sqle = new SQLException("Error executing the query: " + sqlStmt, e);
					throw sqle;
				}

			}
		}

		// beetle 3849 - Need to change the cursor name to what
		// JCC thinks it will be, since there is no way in the
		// protocol to communicate the actual cursor name. JCC keeps
		// a mapping from the client cursor names to the DB2 style cursor names
		if (cursorName != null)// cursorName not null means we are dealing with
								// dynamic pacakges
			ps.setCursorName(cursorName);
		if (isolationSet)
			database.setPrepareIsolation(saveIsolationLevel);

		versionCounter = ((EnginePreparedStatement) ps).getVersionCounter();

		return ps;

	}

	public void close() throws SQLException {
		if (lcc != null) {
			ContextManager cmanager = lcc.getContextManager();
			if (cmanager != null) {
				CPaaSQueryContext cpaasctx = (CPaaSQueryContext) cmanager.getContext("cpaas_id");
				if (cpaasctx != null) {
					QueryContext ctx = CloudMdSQLManager.getInstance().getQueryContext(cpaasctx.getIdQuery());
					if (ctx != null) {
						try {
							lcc.internalCommit(true);
							CloudMdSQLManager.getInstance().removeQueryContext(ctx.getId(), getConnection());
							lcc.internalCommit(true);
						} catch (Exception e) {
							throw new SQLException(e);
						}
					}
					lcc.getContextManager().popContext(cpaasctx);
				}
			}

		}
		super.close();

	}

	public PreparedStatement prepareNamedTable(String sqlStmt) throws SQLException {
		return super.prepare(sqlStmt);
	}

	public long executeLargeUpdate(String sql) throws SQLException {
		return this.getStatement().executeLargeUpdate(sql);
	}

}
