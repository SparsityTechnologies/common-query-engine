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

public class CPaaSQueryContext implements Context{

	private Long idQuery;
	private ContextManager ctxManager;
	private String id;

	
	public CPaaSQueryContext(Long idQuery, ContextManager ctxManager, String id){
		this.idQuery = idQuery;
		this.ctxManager = ctxManager;
		this.id = id;
	}
	
	@Override
	public ContextManager getContextManager() {
		return ctxManager;
	}

	@Override
	public String getIdName() {		
		return id;
	}
	
	public Long getIdQuery(){
		return idQuery;
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
