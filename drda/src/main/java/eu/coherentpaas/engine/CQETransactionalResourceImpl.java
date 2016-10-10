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
package eu.coherentpaas.engine;

import java.sql.SQLException;
import java.util.Properties;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.context.Context;
import org.apache.derby.impl.jdbc.InternalDriverInterface;
import org.apache.derby.impl.jdbc.TransactionResourceImpl;

import eu.coherentpaas.cqe.server.CloudMdSQLManager;
import eu.coherentpaas.transactionmanager.exception.TransactionManagerException;

public class CQETransactionalResourceImpl extends TransactionResourceImpl {

	public CQETransactionalResourceImpl(InternalDriverInterface driver, String url, Properties info)
			throws SQLException {
		super(driver, url, info);
		String txId = info.getProperty("txId");
		if (txId != null) {
			try {
				Context ctxt = cm.getContext("tm_cqe");
				if (ctxt == null) {
					Long id =  Long.parseLong(txId);
					this.cm.pushContext(new TransactionContext(CloudMdSQLManager.getInstance().getTransaction(id),
							this.cm, "tm_cqe",id));
				}
			} catch (TransactionManagerException e) {
				throw new RuntimeException("Error creating the transaction ctx", e);
			}
		}
	}
	
	@Override
	public boolean isIdle(){
		Context ctxt = cm.getContext("tm_cqe");
		if (ctxt != null) {
			try {
				((TransactionContext) ctxt).commit();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return true;
	}

	@Override
	public void startTransaction() throws StandardException, SQLException {
		super.startTransaction();
		try {
			Context ctxt = cm.getContext("tm_cqe");
			if (ctxt == null) {
				this.cm.pushContext(
						new TransactionContext(CloudMdSQLManager.getInstance().startTransaction(), this.cm, "tm_cqe"));			
			}
			else{
				((TransactionContext) ctxt).start();
			}
		} catch (TransactionManagerException e) {
			throw new StandardException("Error creating the transaction ctx", e, null);
		}
	}

	@Override
	public void commit() throws StandardException {
		super.commit();		
		Context ctxt = cm.getContext("tm_cqe");
		if (ctxt != null) {
			try {
				if(((TransactionContext) ctxt).commit()){
					((TransactionContext) ctxt).setTransactionContext(CloudMdSQLManager.getInstance().startTransaction());
				}
			} catch (Exception e) {
				throw new StandardException("Error commiting in the HTM", e, null);
			}
		}
	}

	@Override
	public void rollback() throws StandardException {
		super.rollback();

		Context ctxt = cm.getContext("tm_cqe");
		if (ctxt != null) {
			try {
				if(((TransactionContext) ctxt).rollback()){
					((TransactionContext) ctxt).setTransactionContext(CloudMdSQLManager.getInstance().startTransaction());
				}
			} catch (Exception e) {
				throw new StandardException("Error applying abort in the HTM", e, null);
			}

		}
	}

}
