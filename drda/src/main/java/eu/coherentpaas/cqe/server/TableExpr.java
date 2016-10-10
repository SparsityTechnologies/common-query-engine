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

import eu.coherentpaas.cqe.QueryPlan;


public class TableExpr {

	private String name;
	
	private QueryPlan plan;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public QueryPlan getPlan() {
		return plan;
	}

	public void setPlan(QueryPlan plan) {
		this.plan = plan;
	}
	
	
}
