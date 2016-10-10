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

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.context.Context;
import org.apache.derby.iapi.services.context.ContextManager;

import eu.coherentpaas.transactionmanager.client.TxnCtx;
import eu.coherentpaas.transactionmanager.exception.TransactionManagerException;

public class TransactionContext implements Context {

	private TxnCtx transactionContext;
	private ContextManager ctxManager;
	private String id;
	private Long txId;
	private TransactionStatus status;
	
	public TransactionContext(TxnCtx transactionContext, ContextManager ctxManager, String id, Long txId) {
		this(transactionContext, ctxManager, id);
		this.txId = txId;
	}

	public TransactionContext(TxnCtx transactionContext, ContextManager ctxManager, String id) {
		this.transactionContext = transactionContext;
		this.ctxManager = ctxManager;
		this.id = id;
		txId = null;
		status = TransactionStatus.CONNECT;
	}

	public TxnCtx getTransactionContext() {
		return transactionContext;
	}
	
	public Long getTxId(){
		return txId;
	}
	
	public boolean isShared(){
		return txId != null;
	}
	
	public void setTransactionContext(TxnCtx transactionContext){
		this.transactionContext = transactionContext;
		status = TransactionStatus.COMMIT;
	}

	@Override
	public ContextManager getContextManager() {
		return ctxManager;
	}

	@Override
	public String getIdName() {
		return id;
	}
	
	public void start(){
		if(status.equals(TransactionStatus.CONNECT)){
			status = TransactionStatus.CHECK_USER_IS_NOT_A_ROLE;
		}
		else if(status.equals(TransactionStatus.CHECK_USER_IS_NOT_A_ROLE)){
			status = TransactionStatus.COMMIT;
		}
	}
	
	public boolean commit() throws TransactionManagerException{
		if (status.equals(TransactionStatus.COMMIT) || !isShared()){
			transactionContext.commit();
			return true;
		}		
		return false;
	}
	
	public boolean rollback() throws TransactionManagerException{
		if (status.equals(TransactionStatus.COMMIT) || !isShared()){
			if(!transactionContext.isCommitted()){
				transactionContext.abort();
				return true;
			}
		}
		
		return false;
	}

	@Override
	public void cleanupOnError(Throwable error) throws StandardException {
		// TODO Auto-generated method stub

	}

	@Override
	public void pushMe() {
		getContextManager().pushContext(this);
	}

	@Override
	public void popMe() {
		getContextManager().popContext(this);
	}

	@Override
	public boolean isLastHandler(int severity) {
		// TODO Auto-generated method stub
		return false;
	}

}
