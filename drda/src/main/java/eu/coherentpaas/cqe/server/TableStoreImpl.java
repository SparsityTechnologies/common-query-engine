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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class TableStoreImpl implements TableStore {

	private ObjectOutputStream os = null;

	private ObjectInputStream is = null;

	private File file;

	public TableStoreImpl() throws IOException {
		file = File.createTempFile("tableStore", "data");
	}

	@Override
	public void writeNextRow(Object[][] row) throws Exception {
		if (os == null) {
			os = new ObjectOutputStream(new FileOutputStream(file));
		}
		os.writeObject(row);
		os.reset();
		os.flush();
	}

	@Override
	public Object[][] readNextRow() throws Exception {
		if (os != null) {
			os.close();
			os = null;
		}
		if (is == null) {
			is = new ObjectInputStream(new FileInputStream(file));
		}
		if (is.available() > 0) {
			return (Object[][]) is.readObject();
		}
		else{
			is.close();
			is = null;
		}
		return null;
	}

	@Override
	public void close() throws IOException {
		if (os != null) {
			os.close();
			os = null;
		}
		if (is != null) {
			is.close();
			is = null;
		}
		if (file != null) {
			file.delete();
			file = null;
		}
	}

	@Override
	public void init() throws Exception {
		// TODO Auto-generated method stub
		
	}

}
