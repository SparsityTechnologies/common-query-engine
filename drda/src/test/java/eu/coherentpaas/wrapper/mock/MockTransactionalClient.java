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
package eu.coherentpaas.wrapper.mock;

import eu.coherentpaas.transactionmanager.client.TxnCtx;
import eu.coherentpaas.transactionmanager.datastore.DataStoreId;
import eu.coherentpaas.transactionmanager.datastore.TransactionalDSClient;
import eu.coherentpaas.transactionmanager.exception.DataStoreException;

public class MockTransactionalClient implements TransactionalDSClient {

	public static boolean committed = false;
	
	
	public void applyWS() throws DataStoreException {
		committed = true;
		System.out.println("applyWS into the mock tx client");
	}

	@Override
	public long applyWS(TxnCtx txnCtx) {
		committed = true;
		System.out.println("applyWS into the mock tx client");
		return 0;
	}

	
	public void rollback() {
		System.out.println("rollback into the mock tx client");
	}

	@Override
	public void rollback(TxnCtx txnCtx) {
		System.out.println("rollback into the mock tx client");
	}

	
	public byte[] getWS() throws DataStoreException {
		System.out.println("getWS into the mock tx client");
		return null;
	}

	@Override
	public byte[] getWS(TxnCtx txnCts) throws DataStoreException {
		System.out.println("getWS into the mock tx client");
		return null;
	}

	@Override
	public DataStoreId getDataStoreID() {
		return DataStoreId.DUMMY;
	}

	@Override
	public void redoWS(long ct, byte[] ws) throws DataStoreException {
		System.out.println("redoWS into the mock tx client");
	}

	@Override
	public void unleash(TxnCtx txnCtx) throws DataStoreException {
		committed = true;
		System.out.println("unleash into the mock tx client");
		
	}

	@Override
	public void notify(TxnCtx arg0) throws DataStoreException {
		System.out.println("begin tx");
		
	}

	@Override
	public void garbageCollect(long arg0) {
		// TODO Auto-generated method stub
		
	}

}
