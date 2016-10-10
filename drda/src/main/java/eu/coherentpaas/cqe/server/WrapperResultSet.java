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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

import eu.coherentpaas.cqe.CQEException;

public class WrapperResultSet implements ResultSet {

	private eu.coherentpaas.cqe.ResultSet rs;
	private Object[][] next = null;
	private Map<String, Integer> columns;
	private int currentRow = 0;
	private int totalRows = 0;
	private TableStore tt = null;

	private boolean setUseCache;
	private String tableName = "anonymous";

	private long execTime = 0;
	private String id = null;
	private QueryContext ctx;
	private boolean isClosed = false;

	public WrapperResultSet(QueryContext ctx, String tableName, eu.coherentpaas.cqe.ResultSet rs) throws Exception {
		this(ctx.getColumnIndexes(tableName), rs);
		this.tableName = tableName;
		tt = ctx.getTableStore(tableName);
		this.ctx = ctx;

	}

	public void setId(String id) {
		this.id = id;
	}

	public WrapperResultSet(Map<String, Integer> columns, eu.coherentpaas.cqe.ResultSet rs) {
		this.columns = columns;
		this.rs = rs;
	}

	public boolean isWrapperFor(Class<?> arg0) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	public <T> T unwrap(Class<T> arg0) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean absolute(int row) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	public void afterLast() throws SQLException {
		// TODO Auto-generated method stub

	}

	public void beforeFirst() throws SQLException {
		// TODO Auto-generated method stub

	}

	public void cancelRowUpdates() throws SQLException {
		// TODO Auto-generated method stub

	}

	public void clearWarnings() throws SQLException {
		// TODO Auto-generated method stub

	}

	public void close() throws SQLException {

		if (rs != null) {
			try {
				rs.close();
				if (tt != null) {
					tt.init();
				}
				isClosed = true;
				next = null;
			} catch (Exception e) {
				throw new SQLException(e);
			}
		}
		/*
		 * if(id != null){ this.ctx.removeResultSet(id); }
		 */
	}

	public void deleteRow() throws SQLException {
		// TODO Auto-generated method stub

	}

	public int findColumn(String columnLabel) throws SQLException {
		Integer index = columns.get(columnLabel);
		return index + 1;
	}

	public boolean first() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	public Array getArray(int columnIndex) throws SQLException {
		return (Array) next[currentRow][columnIndex - 1];
	}

	public Array getArray(String columnLabel) throws SQLException {
		Integer index = columns.get(columnLabel);
		if (index != null) {
			return (Array) next[currentRow][index];
		}
		return null;
	}

	public InputStream getAsciiStream(int columnIndex) throws SQLException {
		return (InputStream) next[currentRow][columnIndex - 1];
	}

	public InputStream getAsciiStream(String columnLabel) throws SQLException {
		Integer index = columns.get(columnLabel);
		if (index != null) {
			return (InputStream) next[currentRow][index];
		}
		return null;
	}

	public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
		return (BigDecimal) next[currentRow][columnIndex - 1];
	}

	public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
		Integer index = columns.get(columnLabel);
		if (index != null) {
			return (BigDecimal) next[currentRow][index];
		}
		return null;
	}

	public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
		BigDecimal result = (BigDecimal) next[currentRow][columnIndex - 1];
		return result.setScale(scale, BigDecimal.ROUND_UNNECESSARY);
	}

	public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
		Integer index = columns.get(columnLabel);
		if (index != null) {
			BigDecimal result = (BigDecimal) next[currentRow][index];
			return result.setScale(scale, BigDecimal.ROUND_UNNECESSARY);
		}
		return null;
	}

	public InputStream getBinaryStream(int columnIndex) throws SQLException {
		return (InputStream) next[currentRow][columnIndex - 1];
	}

	public InputStream getBinaryStream(String columnLabel) throws SQLException {
		Integer index = columns.get(columnLabel);
		if (index != null) {
			return (InputStream) next[currentRow][index];
		}
		return null;
	}

	public Blob getBlob(int columnIndex) throws SQLException {
		return (Blob) next[currentRow][columnIndex - 1];
	}

	public Blob getBlob(String columnLabel) throws SQLException {
		Integer index = columns.get(columnLabel);
		if (index != null) {
			return (Blob) next[currentRow][index];
		}
		return null;
	}

	public boolean getBoolean(int columnIndex) throws SQLException {
		return (Boolean) next[currentRow][columnIndex - 1];
	}

	public boolean getBoolean(String columnLabel) throws SQLException {
		Integer index = columns.get(columnLabel);
		if (index != null) {
			return (Boolean) next[currentRow][index];
		}
		return false;
	}

	public byte getByte(int columnIndex) throws SQLException {
		return (Byte) next[currentRow][columnIndex - 1];
	}

	public byte getByte(String columnLabel) throws SQLException {
		Integer index = columns.get(columnLabel);
		if (index != null) {
			return (Byte) next[currentRow][index];
		}
		return 0;
	}

	public byte[] getBytes(int columnIndex) throws SQLException {
		return (byte[]) next[currentRow][columnIndex - 1];
	}

	public byte[] getBytes(String columnLabel) throws SQLException {
		Integer index = columns.get(columnLabel);
		if (index != null) {
			return (byte[]) next[currentRow][index];
		}
		return new byte[0];
	}

	public Reader getCharacterStream(int columnIndex) throws SQLException {
		return (Reader) next[currentRow][columnIndex - 1];
	}

	public Reader getCharacterStream(String columnLabel) throws SQLException {
		Integer index = columns.get(columnLabel);
		if (index != null) {
			return (Reader) next[currentRow][index];
		}
		return null;
	}

	public Clob getClob(int columnIndex) throws SQLException {
		return (Clob) next[currentRow][columnIndex - 1];
	}

	public Clob getClob(String columnLabel) throws SQLException {
		Integer index = columns.get(columnLabel);
		if (index != null) {
			return (Clob) next[currentRow][index];
		}
		return null;
	}

	public int getConcurrency() throws SQLException {
		return 1;
	}

	public String getCursorName() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public Date getDate(int columnIndex) throws SQLException {
		Object o = next[currentRow][columnIndex - 1];
		if (o instanceof Date) {
			return (Date) o;
		} else if (o instanceof Timestamp) {
			Timestamp tm = (Timestamp) o;
			Date d = new Date(tm.getTime());
			return d;
		}
		return null;
	}

	public Date getDate(String columnLabel) throws SQLException {
		Integer index = columns.get(columnLabel);
		if (index != null) {
			Object o = next[currentRow][index];
			if (o instanceof Date) {
				return (Date) o;
			} else if (o instanceof Timestamp) {
				Timestamp tm = (Timestamp) o;
				Date d = new Date(tm.getTime());
				return d;
			}
		}
		return null;
	}

	public Date getDate(int columnIndex, Calendar cal) throws SQLException {
		return (Date) next[currentRow][columnIndex - 1];
	}

	public Date getDate(String columnLabel, Calendar cal) throws SQLException {
		Integer index = columns.get(columnLabel);
		if (index != null) {
			return (Date) next[currentRow][index];
		}
		return null;
	}

	public double getDouble(int columnIndex) throws SQLException {
		Object value = next[currentRow][columnIndex - 1];
		if (value == null) {
			return 0;
		}
		if (value instanceof Float) {
			return ((Float) value).doubleValue();
		}
		return (Double) value;
	}

	public double getDouble(String columnLabel) throws SQLException {
		Integer index = columns.get(columnLabel);
		if (index != null) {
			Object o = next[currentRow][index];
			if (o instanceof Float) {
				return ((Float) o).doubleValue();
			}
			return (Double) o;
		}
		return 0;
	}

	public int getFetchDirection() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	public int getFetchSize() throws SQLException {
		return next.length;
	}

	public float getFloat(int columnIndex) throws SQLException {

		Object o = next[currentRow][columnIndex - 1];
		if (o instanceof Double) {
			return ((Double) o).floatValue();
		}
		return (Float) next[currentRow][columnIndex - 1];
	}

	public float getFloat(String columnLabel) throws SQLException {
		Integer index = columns.get(columnLabel);
		if (index != null) {
			Object o = next[currentRow][index];
			if (o instanceof Double) {
				return ((Double) o).floatValue();
			}
			return (Float) o;
		}
		return 0;
	}

	public int getHoldability() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	public int getInt(int columnIndex) throws SQLException {

		Object o = next[currentRow][columnIndex - 1];
		if (o instanceof Long) {
			return ((Long) o).intValue();
		}
		if (o instanceof Double) {
			return ((Double) o).intValue();
		}
		return (Integer) next[currentRow][columnIndex - 1];
	}

	public int getInt(String columnLabel) throws SQLException {
		Integer index = columns.get(columnLabel);
		if (index != null) {
			Object o = next[currentRow][index];
			if (o instanceof Long) {
				return ((Long) o).intValue();
			}
			if (o instanceof Double) {
				return ((Double) o).intValue();
			}
			return (Integer) o;

		}
		return 0;
	}

	public long getLong(int columnIndex) throws SQLException {
		Object aux = next[currentRow][columnIndex - 1];
		if (aux instanceof BigInteger) {
			return ((BigInteger) aux).longValue();
		}
		if (aux instanceof Integer) {
			return ((Integer) aux).longValue();
		}
		else if (aux instanceof Double) {
			return ((Double) aux).longValue();
		}
		return (Long) next[currentRow][columnIndex - 1];
	}

	public long getLong(String columnLabel) throws SQLException {
		Integer index = columns.get(columnLabel);
		if (index != null) {
			Object aux = next[currentRow][index];
			if (aux instanceof BigInteger) {
				return ((BigInteger) aux).longValue();
			}
			if (aux instanceof Integer) {
				return ((Integer) aux).longValue();
			}
			else if (aux instanceof Double) {
				return ((Double) aux).longValue();
			}
			return (Long) next[currentRow][index];
		}
		return 0;
	}

	public ResultSetMetaData getMetaData() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public Reader getNCharacterStream(int columnIndex) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public Reader getNCharacterStream(String columnLabel) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public NClob getNClob(int columnIndex) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public NClob getNClob(String columnLabel) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public String getNString(int columnIndex) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public String getNString(String columnLabel) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public Object getObject(int columnIndex) throws SQLException {
		return next[currentRow][columnIndex - 1];
	}

	public Object getObject(String columnLabel) throws SQLException {
		Integer index = columns.get(columnLabel);
		if (index != null) {
			return next[currentRow][index];
		}
		return null;
	}

	public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public Ref getRef(int columnIndex) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public Ref getRef(String columnLabel) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public int getRow() throws SQLException {
		return currentRow;
	}

	public RowId getRowId(int columnIndex) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public RowId getRowId(String columnLabel) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public SQLXML getSQLXML(int columnIndex) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public SQLXML getSQLXML(String columnLabel) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public short getShort(int columnIndex) throws SQLException {
		return (Short) next[currentRow][columnIndex];
	}

	public short getShort(String columnLabel) throws SQLException {
		Integer index = columns.get(columnLabel);
		if (index != null) {
			return (Short) next[currentRow][index];
		}
		return 0;
	}

	public Statement getStatement() throws SQLException {

		return null;
	}

	public String getString(int columnIndex) throws SQLException {
		Object o = next[currentRow][columnIndex - 1];
		if (o != null) {
			return o.toString();
		} else {
			return null;
		}
	}

	public String getString(String columnLabel) throws SQLException {
		Integer index = columns.get(columnLabel);
		if (index != null) {
			Object o = next[currentRow][index];
			if (o != null) {
				return o.toString();
			} else {
				return null;
			}
		}
		return "";

	}

	public Time getTime(int columnIndex) throws SQLException {
		return (Time) next[currentRow][columnIndex];
	}

	public Time getTime(String columnLabel) throws SQLException {
		Integer index = columns.get(columnLabel);
		if (index != null) {
			return (Time) next[currentRow][index];
		}
		return null;
	}

	public Time getTime(int columnIndex, Calendar cal) throws SQLException {
		return (Time) next[currentRow][columnIndex];
	}

	public Time getTime(String columnLabel, Calendar cal) throws SQLException {
		Integer index = columns.get(columnLabel);
		if (index != null) {
			return (Time) next[currentRow][index];
		}
		return null;
	}

	public Timestamp getTimestamp(int columnIndex) throws SQLException {
		Object o = next[currentRow][columnIndex - 1];
		if (o instanceof Date) {
			Date date = (Date) o;
			Timestamp tm = new Timestamp(date.getTime());
			return tm;
		} else if (o instanceof Timestamp) {
			return (Timestamp) o;
		}
		return null;
	}

	public Timestamp getTimestamp(String columnLabel) throws SQLException {
		Integer index = columns.get(columnLabel);
		if (index != null) {
			Object o = next[currentRow][index];
			if (o instanceof Date) {
				Date date = (Date) o;
				Timestamp tm = new Timestamp(date.getTime());
				return tm;
			} else if (o instanceof Timestamp) {
				return (Timestamp) o;
			}
		}
		return null;
	}

	public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
		return (Timestamp) next[currentRow][columnIndex];
	}

	public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
		Integer index = columns.get(columnLabel);
		if (index != null) {
			return (Timestamp) next[currentRow][index];
		}
		return null;
	}

	public int getType() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	public URL getURL(int columnIndex) throws SQLException {
		return (URL) next[currentRow][columnIndex];
	}

	public URL getURL(String columnLabel) throws SQLException {
		Integer index = columns.get(columnLabel);
		if (index != null) {
			return (URL) next[currentRow][index];
		}
		return null;
	}

	public InputStream getUnicodeStream(int columnIndex) throws SQLException {
		return (InputStream) next[currentRow][columnIndex];
	}

	public InputStream getUnicodeStream(String columnLabel) throws SQLException {
		Integer index = columns.get(columnLabel);
		if (index != null) {
			return (InputStream) next[currentRow][index];
		}
		return null;
	}

	public SQLWarning getWarnings() throws SQLException {
		return null;
	}

	public void insertRow() throws SQLException {
		// TODO Auto-generated method stub

	}

	public boolean isAfterLast() throws SQLException {
		return currentRow == next.length;
	}

	public boolean isBeforeFirst() throws SQLException {
		return currentRow == 0;
	}

	public boolean isClosed() throws SQLException {
		
		return isClosed;
	}

	public boolean isFirst() throws SQLException {

		return currentRow == 1;
	}

	public boolean isLast() throws SQLException {

		try {
			return currentRow == (this.rs.getRowCount() - 1);
		} catch (CQEException e) {
			SQLException ex = new SQLException();
			ex.setStackTrace(e.getStackTrace());
			throw ex;
		}
	}

	public boolean last() throws SQLException {
		return isLast();
	}

	public void moveToCurrentRow() throws SQLException {
		// TODO Auto-generated method stub

	}

	public void moveToInsertRow() throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setUseCache(boolean setUseCache) {
		this.setUseCache = setUseCache;
		isClosed = false;
	}

	public boolean next() throws SQLException {
		if (!isClosed) {
			try {
				if (next == null || currentRow == next.length - 1) {
					if (!setUseCache) {
						long time1 = System.currentTimeMillis();
						next = this.rs.next();
						if(next != null){
							totalRows++;
						}
						long current = System.currentTimeMillis();
						execTime += current - time1;
						tt.writeNextRow(next);
					} else {
						next = tt.readNextRow();
					}
					currentRow = 0;
				} else {
					if (currentRow < next.length) {
						currentRow++;
						if (!setUseCache) {
							totalRows++;
						}
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
				SQLException ex = new SQLException();
				ex.setStackTrace(e.getStackTrace());
				throw ex;
			}
		}
		boolean hasNext = next != null && next.length > 0;
		if (!hasNext && !setUseCache) {
			System.out.println(
					"The query [" + tableName + "] exec time is " + execTime + " and has returned " + totalRows);
			try {
				File file = new File(tableName + "_profile.txt");
				BufferedWriter writer = null;
				FileWriter fw = null;
				try {
					fw = new FileWriter(file, true);
					writer = new BufferedWriter(fw);
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
				writer.write(tableName + "|" + execTime + "\n");
				writer.close();
				fw.close();
			} catch (IOException e) {
				SQLException ex = new SQLException();
				ex.setStackTrace(e.getStackTrace());
				throw ex;
			}

		}
		return hasNext;
	}

	public boolean previous() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	public void refreshRow() throws SQLException {
		// TODO Auto-generated method stub

	}

	public boolean relative(int rows) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean rowDeleted() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean rowInserted() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean rowUpdated() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	public void setFetchDirection(int direction) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setFetchSize(int rows) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateArray(int columnIndex, Array x) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateArray(String columnLabel, Array x) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateBlob(int columnIndex, Blob x) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateBlob(String columnLabel, Blob x) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateBoolean(int columnIndex, boolean x) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateBoolean(String columnLabel, boolean x) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateByte(int columnIndex, byte x) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateByte(String columnLabel, byte x) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateBytes(int columnIndex, byte[] x) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateBytes(String columnLabel, byte[] x) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateClob(int columnIndex, Clob x) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateClob(String columnLabel, Clob x) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateClob(int columnIndex, Reader reader) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateClob(String columnLabel, Reader reader) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateDate(int columnIndex, Date x) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateDate(String columnLabel, Date x) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateDouble(int columnIndex, double x) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateDouble(String columnLabel, double x) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateFloat(int columnIndex, float x) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateFloat(String columnLabel, float x) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateInt(int columnIndex, int x) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateInt(String columnLabel, int x) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateLong(int columnIndex, long x) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateLong(String columnLabel, long x) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateNClob(int columnIndex, Reader reader) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateNClob(String columnLabel, Reader reader) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateNString(int columnIndex, String nString) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateNString(String columnLabel, String nString) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateNull(int columnIndex) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateNull(String columnLabel) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateObject(int columnIndex, Object x) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateObject(String columnLabel, Object x) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateRef(int columnIndex, Ref x) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateRef(String columnLabel, Ref x) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateRow() throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateRowId(int columnIndex, RowId x) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateRowId(String columnLabel, RowId x) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateShort(int columnIndex, short x) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateShort(String columnLabel, short x) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateString(int columnIndex, String x) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateString(String columnLabel, String x) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateTime(int columnIndex, Time x) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateTime(String columnLabel, Time x) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
		// TODO Auto-generated method stub

	}

	public boolean wasNull() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

}
