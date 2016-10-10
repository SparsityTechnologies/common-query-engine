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

import java.sql.SQLException;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.context.Context;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.services.loader.GeneratedClass;
import org.apache.derby.iapi.sql.PreparedStatement;
import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.conn.StatementContext;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.impl.sql.CursorInfo;
import org.apache.derby.impl.sql.GenericPreparedStatement;
import org.apache.derby.impl.sql.GenericStatement;
import org.apache.derby.impl.sql.compile.StatementNode;

import eu.coherentpaas.cqe.TransformQueryPlan;
import eu.coherentpaas.engine.CPaaSQueryContext;
import eu.coherentpaas.engine.TransactionContext;

public class CloudMdSQL2DerbyStatement extends GenericStatement {

	private CloudMdSQLStatement cloudMdSQLStatement;

	private long id = 0;

	private String code;

	private String name;

	private GenericPreparedStatement preparedStmt;

	public CloudMdSQL2DerbyStatement(CloudMdSQLStatement cloudMdSQLStatement, SchemaDescriptor compilationSchema,
			String statementText, boolean isForReadOnly) {
		super(compilationSchema, "", isForReadOnly);
		this.cloudMdSQLStatement = cloudMdSQLStatement;
		this.code = statementText;
	}

	public CloudMdSQL2DerbyStatement(String name, QueryContext ctx, CloudMdSQLStatement cloudMdSQLStatement,
			SchemaDescriptor compilationSchema) {
		super(compilationSchema, "", true);
		this.cloudMdSQLStatement = cloudMdSQLStatement;
		this.id = ctx.getId();
		this.name = name;
	}

	public PreparedStatement prepare(TransformQueryPlan plan) throws StandardException {
		LanguageConnectionContext lcc = cloudMdSQLStatement.getLanguageConnectionContext();

		StatementContext statementContext = lcc.pushStatementContext(true, isForReadOnly, "", null, false, 0L);

		CompilerContext cc = lcc.pushCompilerContext(getSchemaDescriptor());
		setPrepareIsolationLevel(lcc.getPrepareIsolationLevel());

		GenericPreparedStatement preparedStmt = new GenericPreparedStatement(this);
		StatementNode qt = null;

		CPaaSQueryContext cpaasQueryCtx = (CPaaSQueryContext) lcc.getContextManager().getContext("cpaas_id");

		try {
			synchronized (preparedStmt) {
				qt = CloudMdSQLManager.getInstance().getQueryContext(cpaasQueryCtx.getIdQuery()).compileToSQL(name,
						plan.getPlan(), cc);
			}
		} catch (Exception e) {
			lcc.popCompilerContext(cc);
			if (preparedStmt != null) {
				preparedStmt.endCompiling();
			}
			lcc.commitNestedTransaction();
			throw new StandardException("Error compiling the cloudMdSQL expression", e, null);
		}
		compile(statementContext, cc, preparedStmt, lcc, qt);
		lcc.popCompilerContext(cc);
		if (statementContext != null)
			lcc.popStatementContext(statementContext, null);

		return preparedStmt;
	}

	public void compile(StatementContext statementContext, CompilerContext cc, GenericPreparedStatement preparedStmt,
			LanguageConnectionContext lcc, StatementNode qt) throws StandardException {
		DataDictionary dataDictionary = null;
		int ddMode = 0;
		try {
			cc.setCurrentDependent(preparedStmt);

			preparedStmt.referencesSessionSchema(qt);

			preparedStmt.beginCompiling();

			lcc.beginNestedTransaction(true);

			dataDictionary = lcc.getDataDictionary();

			ddMode = dataDictionary == null ? 0 : dataDictionary.startReading(lcc);

			qt.bindStatement();

			qt.optimizeStatement();

			GeneratedClass ac = qt.generate(preparedStmt.getByteCodeSaver());

			preparedStmt.setConstantAction(qt.makeConstantAction());
			preparedStmt.setSavedObjects(cc.getSavedObjects());
			preparedStmt.setRequiredPermissionsList(cc.getRequiredPermissionsList());
			preparedStmt.incrementVersionCounter();
			preparedStmt.setActivationClass(ac);
			preparedStmt.setNeedsSavepoint(qt.needsSavepoint());
			preparedStmt.setCursorInfo((CursorInfo) cc.getCursorInfo());
			preparedStmt.setIsAtomic(qt.isAtomic());
			preparedStmt.setExecuteStatementNameAndSchema(qt.executeStatementName(), qt.executeSchemaName());
			preparedStmt.setSPSName(qt.getSPSName());
			preparedStmt.completeCompile(qt);
			preparedStmt.setCompileTimeWarnings(cc.getWarnings());
			setPreparedStatement(preparedStmt);

			if (statementContext != null) {
				lcc.popStatementContext(statementContext, null);
			}

		} catch (Exception e) {
			e.printStackTrace();
			throw new StandardException("Error compiling the cloudMdSQL expression = [" + code + "]", e, null);
		} finally // for block introduced by pushCompilerContext()
		{

			if (dataDictionary != null)
				dataDictionary.doneReading(ddMode, lcc);

			lcc.popCompilerContext(cc);
			if (preparedStmt != null) {
				preparedStmt.endCompiling();
			}
			lcc.commitNestedTransaction();
		}
		
	}

	@Override
	public PreparedStatement prepare(LanguageConnectionContext lcc, boolean forMetaData) throws StandardException {

		
		if (preparedStmt == null) {
			preparedStmt = new GenericPreparedStatement(this);
		} else if (preparedStmt.upToDate()) {
			return preparedStmt;
		}
	
		StatementContext statementContext = lcc.pushStatementContext(true, isForReadOnly, "", null, false, 0L);

		CompilerContext cc = lcc.pushCompilerContext(getSchemaDescriptor());
		setPrepareIsolationLevel(lcc.getPrepareIsolationLevel());

		TransactionContext transactionCtx = (TransactionContext) lcc.getContextManager().getContext("tm_cqe");
		synchronized (preparedStmt) {

			CloudMdSQLParser parser = new CloudMdSQLParser();
			CloudMdSQLExpr expr = null;
			StatementNode qt = null;

			try {
				String queryLanguage = cloudMdSQLStatement.getConnection().getQueryLanguage();
				if (queryLanguage == null || queryLanguage.equals("cloudmdsql")) {
					expr = parser.parseQueryLanguageExpr(code);
					if (expr.getStatus() != null && expr.getStatus().equals("error")) {
						SQLException sqle = new SQLException(expr.getType() + ":" + code);
						throw sqle;
					}
				} else {
					System.out.println(code);
					expr = parser.parseQueryPlanExpr(code);

				}

				Context ctxOld = lcc.getContextManager().getContext("cpaas_id");
				if (ctxOld != null) {
					lcc.getContextManager().popContext(ctxOld);
				}

				QueryContext ctx = CloudMdSQLManager.getInstance()
						.buildQueryContext(transactionCtx.getTransactionContext(), cloudMdSQLStatement, code);

				CPaaSQueryContext cpaasQueryCtx = new CPaaSQueryContext(ctx.getId(), lcc.getContextManager(),
						"cpaas_id");
				lcc.getContextManager().pushContext(cpaasQueryCtx);

				org.apache.derby.iapi.services.context.ContextManager cm = ctx.getCloudMdSQLStatement()
						.getLanguageConnectionContext().getContextManager();
				ContextService.getFactory().setCurrentContextManager(cm);

				if (expr != null) {

					qt = ctx.compileToSQL(expr, cc);
					
					
				}

			} catch (Exception e) {
				e.printStackTrace();
				lcc.popCompilerContext(cc);
				if (preparedStmt != null) {
					preparedStmt.endCompiling();
				}
				lcc.commitNestedTransaction();
				throw new StandardException("Error compiling the cloudMdSQL expression = [" + code + "]", e, null);

			}
			compile(statementContext, cc, preparedStmt, lcc, qt);
			if (statementContext != null) {
				lcc.popStatementContext(statementContext, null);
			}
			lcc.popCompilerContext(cc);

		}
		return preparedStmt;
	}

}
