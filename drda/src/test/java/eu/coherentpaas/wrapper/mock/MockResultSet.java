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

import eu.coherentpaas.cqe.CQEException;
import eu.coherentpaas.cqe.ResultSet;

public class MockResultSet implements ResultSet {
	private boolean hasNext = true;

	@Override
	public int getRowCount() throws CQEException {
		return 2;
	}

	@Override
	public Object[][] next() throws CQEException {

		Object[][] result = null;
		if (hasNext) {
			result = new Object[][] { new Object[] {1, "b" },
					new Object[] { 2, "c" } };
			hasNext = false;
		}

		return result;
	}

	@Override
	public Object[][] next(int max) throws CQEException {
		return next();
	}

	@Override
	public void close() throws CQEException {
	}

}
