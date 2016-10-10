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

import java.sql.SQLException;

import org.python.core.Py;
import org.python.core.PyException;
import org.python.core.PyGenerator;
import org.python.core.PyObject;
import org.python.core.PyTuple;

import eu.coherentpaas.cqe.CQEException;
import eu.coherentpaas.cqe.ResultSet;

public class PythonResultSet implements ResultSet {

	private PyGenerator generator;

	private PyTuple next = null;

	public PythonResultSet(QueryContext ctxt, String tableName,
			PyGenerator generator) {
		this.generator = generator;
	}

	public void close() {
		if (generator != null) {
			generator.close();
		}

	}

	public double getDouble(int index) throws SQLException {
		PyObject[] tuple = next.getArray();
		return (Double) tuple[index].__tojava__(Double.class);
	}

	public float getFloat(int index) throws SQLException {
		PyObject[] tuple = next.getArray();
		return (Float) tuple[index].__tojava__(Float.class);
	}

	@Override
	public int getRowCount() throws CQEException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Object[][] next() throws CQEException {
		return next(1);

	}

	@Override
	public Object[][] next(int max) throws CQEException {
		Object[][] result = null;
		if(generator == null){
			return result;
		}
		try {
			PyObject aux = generator.next();
			if (aux instanceof PyTuple) {
				next = (PyTuple) aux;
			} else {
				next = new PyTuple(aux);
			}
			if (next == null) {
				return null;
			} else {
				int size = next.size();
				result = new Object[max][];
				for (int i = 0; i < max && next != null; i++) {
					Object[] row = new Object[size];
					for (int j = 0; j < size; j++) {
						row[j] = next.get(j);
					}
					result[i] = row;
					if (i + 1 < max) {
						aux = generator.next();
						if (aux instanceof PyTuple) {
							next = (PyTuple) aux;
						} else {
							next = new PyTuple(aux);
						}
					}
				}
			}

		} catch (PyException e) {
			if (e.match(Py.StopIteration)) {
				return result;
			} else {
				e.printStackTrace();
				throw e;
			}
		}
		return result;
	}

}
