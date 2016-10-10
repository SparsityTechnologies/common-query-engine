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

import org.apache.derby.impl.sql.compile.CountAggregateDefinition;
import org.apache.derby.impl.sql.compile.MaxMinAggregateDefinition;
import org.apache.derby.impl.sql.compile.SumAvgAggregateDefinition;

public enum AggregatorKind {
	
	
	SUM ("SUM", "sum", SumAvgAggregateDefinition.class), 
	
	AVG ("AVG", "avg", SumAvgAggregateDefinition.class), 
	
	COUNT ("COUNT", "count", CountAggregateDefinition.class),
	
	COUNT_STAR ("COUNT(*)", "count( * )", CountAggregateDefinition.class),
	
	MAX ("MAX", "max", MaxMinAggregateDefinition.class), 
	
	MIN ("MIN", "min", MaxMinAggregateDefinition.class);
	
	private String derbyName;
	
	private String cloudMdSQLName;
	
	private Class<?> functionClass;
	
	
	AggregatorKind(String derbyName, String cloudMdSQLName, Class<?> functionClass){
		this.derbyName = derbyName;
		this.cloudMdSQLName = cloudMdSQLName;
		this.functionClass = functionClass;
	}
	
	public static AggregatorKind getAggregatorByCloudMdSQLName(String name){
		AggregatorKind[] values = AggregatorKind.values();
	
		AggregatorKind result = null;
		for(int i = 0; i < values.length && result ==null; i++){
			
			if(values[i].cloudMdSQLName.equals(name)){
				result = values[i];
			}
		}
		return result;
	}
	
	public Class<?> getFunctionClass(){
		return functionClass;
	}
	
	public String getDerbyName(){
		return derbyName;
	}
	
}
