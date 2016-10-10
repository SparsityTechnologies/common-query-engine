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
package org.apache.derby.drda;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.TreeSet;

import org.junit.Assert;
import org.junit.Test;

import eu.coherentpaas.cqe.server.Dependency;
public class DependencyTest {

	@Test
	public void testOrdering(){
		Set<String> list = new LinkedHashSet<String>();
		list.add("t2");
		list.add("t3");
		
		Dependency dep1 = new Dependency("t1", list);
		
		Set<String> list2 = new LinkedHashSet<String>();
		list2.add("t3");
		
		Dependency dep2 = new Dependency("t2", list2);
		
		Dependency dep3 = new Dependency("t3", new LinkedHashSet<String>());
		
		TreeSet<Dependency> dependencies = new TreeSet<Dependency>();
		
		dependencies.add(dep1);
		dependencies.add(dep2);
		dependencies.add(dep3);
		
		Iterator<Dependency> it = dependencies.iterator();
		
		Dependency dep = it.next();
		
		Assert.assertEquals("t3", dep.getTable());
		
		dep = it.next();
		
		Assert.assertEquals("t2", dep.getTable());
		
		dep = it.next();
		
		Assert.assertEquals("t1", dep.getTable());
	}
}
