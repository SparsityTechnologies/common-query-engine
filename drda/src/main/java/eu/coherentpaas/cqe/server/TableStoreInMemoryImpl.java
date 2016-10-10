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

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class TableStoreInMemoryImpl implements TableStore {

	private List<Object[][]> rows = new LinkedList<Object[][]>();
	Iterator<Object[][]> it = null;

	@Override
	public void close() throws IOException {
		rows.clear();
	}

	@Override
	public void writeNextRow(Object[][] row) throws Exception {
		rows.add(row);
	}

	@Override
	public synchronized Object[][] readNextRow() throws Exception {
		
		if (it == null) {
			it = rows.iterator();

		}
		if(it.hasNext()){
			return it.next();
		}
		return null;
	}

	@Override
	public void init() throws Exception {
		it = null;
	}

}
