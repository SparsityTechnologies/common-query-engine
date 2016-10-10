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

import java.util.List;
import java.util.Set;

public class Dependency implements Comparable<Dependency>{

	private String table;
	
	private Set<String> dependencies;

	public Dependency(String table, Set<String> dependencies) {
		super();
		this.table = table;
		this.dependencies = dependencies;
	}

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}

	public Set<String> getDependencies() {
		return dependencies;
	}

	public void setDependencies(Set<String> dependencies) {
		this.dependencies = dependencies;
	}

	@Override
	public int compareTo(Dependency o) {
		if(o.table.equals(table)){
			return 0;
		}
		if(o.dependencies.size() < dependencies.size()){
			//the less value, the more priority
			//who has more priority is who has less dependencies
			return  1; 
		}else{
			return -1;
		}
	}
	
	
}
