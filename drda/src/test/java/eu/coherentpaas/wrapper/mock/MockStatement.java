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

import java.sql.Blob;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;

import com.leanxcale.exception.LeanxcaleException;

import eu.coherentpaas.cqe.CQEException;
import eu.coherentpaas.cqe.QueryPlan;
import eu.coherentpaas.cqe.ResultSet;
import eu.coherentpaas.cqe.Type;
import eu.coherentpaas.cqe.datastore.Parameterized;
import eu.coherentpaas.cqe.datastore.Statement;
import eu.coherentpaas.transactionmanager.client.TxnCtx;

public class MockStatement implements Statement{
	
	private QueryPlan query;
	
	private MockTransactionalClient client;
	
	public MockStatement(){
		client = new MockTransactionalClient();
	}

	@Override
	public void setInt(String parameterName, int value) throws CQEException {
	}

	@Override
	public void setString(String parameterName, String value)
			throws CQEException {
	}

	@Override
	public ResultSet execute(TxnCtx context) throws CQEException,
			InterruptedException {	
		
		try {
			context.associate(client);
			context.markAsUpdateTransaction();
		} catch (LeanxcaleException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return new MockResultSet();
	}

	@Override
	public void close() throws CQEException {
	}

	@Override
	public void useNamedTable(String name, Parameterized table)
			throws CQEException {
	}

	@Override
	public void prepare(QueryPlan query) throws CQEException {		
		this.query = query;
	}

	@Override
	public void setTimestamp(String parameterName, Timestamp value)
			throws CQEException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setArray(String parameterName, ArrayList<?> value)
			throws CQEException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setDictionary(String parameterName, HashMap<?, ?> value)
			throws CQEException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setFloat(String parameterName, Float value) throws CQEException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setDouble(String parameterName, Double value)
			throws CQEException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setBinary(String parameterName, Blob value) throws CQEException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setLong(String parameterName, long value) throws CQEException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Type[] getSignature() throws CQEException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setDate(String parameterName, Date value) throws CQEException {
		// TODO Auto-generated method stub
		
	}

}
