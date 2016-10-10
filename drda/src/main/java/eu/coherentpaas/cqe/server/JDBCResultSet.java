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

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import eu.coherentpaas.cqe.CQEException;
import eu.coherentpaas.cqe.ResultSet;
import eu.coherentpaas.cqe.Type;
import eu.coherentpaas.cqe.CQEException.Severity;

public class JDBCResultSet implements ResultSet {

	private java.sql.ResultSet resultSet;
	private Type[] signature;
	private int colNumber = -1;
	private ResultSetMetaData metadata = null;

	public JDBCResultSet(java.sql.ResultSet rs, Type[] signature) {
		this.resultSet = rs;
		this.signature = signature;
		
	}

	public int getRowCount() throws CQEException {
		return -1;
	}

	public Object[][] next() throws CQEException {
		return next(1000);
	}

	public Object[][] next(int max) throws CQEException {
		try {
			
			if (resultSet == null || resultSet.isClosed())
				return null;
			if(colNumber == -1){
				metadata = resultSet.getMetaData();
				colNumber = metadata.getColumnCount();
			}
			List<Object[]> rows = new ArrayList<Object[]>();

			while (max > 0 && resultSet.next()) {

				Object[] row = new Object[colNumber];
				for (int i = 0; i < colNumber; i++) {
					switch (signature[i]) {
					case INT:
						row[i] = resultSet.getInt(i + 1);
						break;
					case STRING:
						row[i] = resultSet.getString(i + 1);
						break;
					case DOUBLE:
						row[i] = resultSet.getDouble(i + 1);
						break;
					case LONG:
						row[i] = resultSet.getLong(i + 1);
						break;
					case FLOAT:
						row[i] = resultSet.getFloat(i + 1);
						break;
					case TIMESTAMP:
						int type =  metadata.getColumnType(i+1);
						if(type == java.sql.Types.DATE){
						row[i] = resultSet.getDate(i+1);
						}
						else if(type == java.sql.Types.TIMESTAMP){
							row[i] = resultSet.getTimestamp(i+1);
						}
						else if(type == java.sql.Types.TIME){
							row[i] = resultSet.getTime(i+1);
						}
					}
				}
				rows.add(row);
			}

			if (rows.isEmpty())
				return null;

			return rows.toArray(new Object[rows.size()][]);
		} catch (SQLException e) {
			throw new CQEException(Severity.Execution, null, e);
		}
	}

	public void close() throws CQEException {
		try {
			resultSet.close();
		} catch (SQLException e) {
			throw new CQEException(Severity.Execution, null, e);
		}
	}

}
