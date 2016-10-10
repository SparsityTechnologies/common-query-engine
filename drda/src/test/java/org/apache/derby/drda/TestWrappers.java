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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import eu.coherentpaas.cqe.server.CloudMdSQLArray;
import eu.coherentpaas.cqe.server.CloudMdSQLMap;
import eu.coherentpaas.cqe.server.CloudMdSQLMapEntry;

public class TestWrappers {

	private static boolean areTypesInitizalized = false;

	@BeforeClass
	public static void startServer() throws Exception {
		Thread thread = new Thread() {
			@Override
			public void run() {

				CloudMdSQLServer.main(new String[] { "start", "-noSecurityManager" });
			}
		};
		thread.start();
		CloudMdSQLServer server = CloudMdSQLServer.getInstance();
		boolean started = false;
		while (!started) {
			try {
				server.ping();
				started = true;
			} catch (Exception e) {

				Thread.sleep(1000);
				server = CloudMdSQLServer.getInstance();
			}
		}
	}

	@AfterClass
	public static void stopServer() {
		Thread thread = new Thread() {
			@Override
			public void run() {

				CloudMdSQLServer.main(new String[] { "shutdown" });
			}
		};

		thread.start();

	}

	protected Connection createConnection() throws Exception {
		String EXTERNAL_DRIVER = "org.apache.derby.jdbc.ClientDriver";

		Class.forName(EXTERNAL_DRIVER);

		Connection c = DriverManager.getConnection("jdbc:derby://localhost:1527/seconddb;create=true;");
		if (!areTypesInitizalized) {
			initCustomTypes(DriverManager.getConnection("jdbc:derby://localhost:1527/seconddb;create=true;"));
			areTypesInitizalized = true;
		}
		c.setAutoCommit(false);

		return c;

	}

	protected Connection createConnection(Long txId) throws Exception {
		String EXTERNAL_DRIVER = "org.apache.derby.jdbc.ClientDriver";

		Class.forName(EXTERNAL_DRIVER);

		Connection c = DriverManager
				.getConnection("jdbc:derby://localhost:1527/seconddb;create=true;txId=" + txId.toString() + ";");

		if (!areTypesInitizalized) {
			initCustomTypes(DriverManager
					.getConnection("jdbc:derby://localhost:1527/seconddb;create=true;txId=" + txId.toString() + ";"));
			areTypesInitizalized = true;
		}

		c.setAutoCommit(false);

		return c;

	}

	@Test
	public void testDoubleNull() throws Exception {
		Connection conn = createConnection();

		String relevantwords = "relevantwords(row string, qualifier string, word string )@python = {*\tyield ('1', '1', 'rxps') \n"
				+ "\tyield ('1', '2', 'ajaoqwm') \n" + "\tyield ('1', '3', 'jfs') *} \n";
		String scores = "scores( row string, column string, value double)@python = {*\tyield ('ajaoqwm', '115', 62.0) \n"
				+ "\tyield('jfs', '476', 16.0) \n" + "\tyield('jfs', '619', 72.0) \n" + "\tyield('jfs', '845', 7.0) \n"
				+ "\tyield('rxps', '232', 44.0) \n" + "\tyield('rxps', '614', 30.0) \n"
				+ "\tyield('rxps', '73', 36.0) \n" + "\n *} \n";

		String experts = "experts(word string, institutionid string, weight double) = ("
				+ "select relevantwords.word, scores.column, scores.value from scores INNER JOIN relevantwords ON scores.row = relevantwords.word"
				+ ")\n";

		String query = relevantwords + scores + experts;
		query = query + "select word, institutionid, weight from experts";

		PreparedStatement stmt = conn.prepareStatement(query);
		stmt.execute();
		ResultSet rs = stmt.getResultSet();
		while (rs.next()) {
			System.out.println(
					"row " + rs.getRow() + ": " + rs.getString(1) + ", " + rs.getString(2) + ", " + rs.getString(3));
		}
		rs.close();
		stmt.close();
		conn.commit();
		conn.close();
		Assert.assertTrue(true);

	}

	@Test
	public void testWrapperQuery() throws Exception {
		Connection conn = createConnection();
		String t1 = "T1( x int, y string )@python = {* yield (1, 'abc') *} \n";
		String t2 = "T2( x int, y string ) = ( SELECT T1.x, T1.y FROM T1 ) \n";
		String query = t1 + t2 + "SELECT T2.x, T2.y FROM T2 WHERE T2.x = 1 AND T2.x = 2";

		PreparedStatement stmt = conn.prepareStatement(query);

		stmt.execute();

		ResultSet rs = stmt.getResultSet();

		while (rs.next()) {
			System.out.println("row " + rs.getRow() + ": " + rs.getInt(1) + ", " + rs.getString(2));
		}

		rs.close();
		stmt.close();
		conn.commit();
		conn.close();
		Assert.assertTrue(true);
	}

	@Test
	public void testWrapperQuery2() throws Exception {
		Connection conn = createConnection();
		String t1 = "T1( x long, y string )@python = {* yield (1L, 'abc') *} \n";
		String query = t1 + "SELECT T1.x, T1.y FROM T1 WHERE T1.x = 1";

		PreparedStatement stmt = conn.prepareStatement(query);

		stmt.execute();

		ResultSet rs = stmt.getResultSet();

		while (rs.next()) {
			System.out.println("row " + rs.getRow() + ": " + rs.getInt(1) + ", " + rs.getString(2));
		}

		rs.close();
		stmt.close();
		conn.commit();
		conn.close();
		Assert.assertTrue(true);
	}

	@Test
	public void testJoin() throws Exception {
		Connection conn = createConnection();
		testJoin1Con(conn);
		conn.commit();
		conn.close();
	}

	private void testJoin1Con(Connection conn) throws Exception {
		String t1 = "T1( b string )@python = {* yield ('abc') *} \n";
		String t2 = "T2( a int, c string)@python = {* yield (1, 'abc') *} \n";

		String query = t1 + t2;
		query = query + "SELECT T1.b, T2.c FROM T1 JOIN T2 ON T1.b = T2.c";

		PreparedStatement stmt = conn.prepareStatement(query);
		stmt.execute();
		ResultSet rs = stmt.getResultSet();

		while (rs.next()) {
			System.out.println("row " + rs.getRow() + ": " + rs.getString(1) + ", " + rs.getString(2));
		}
		rs.close();
		stmt.close();
		conn.commit();
		Assert.assertTrue(true);
	}

	@Test
	public void testJoin2() throws Exception {
		Connection conn = createConnection();
		testJoin2Con(conn);

		conn.close();
	}

	private void testJoin2Con(Connection conn) throws Exception {
		String t1 = "T1( b string )@python = {* yield ('cdc') *} \n";
		String t2 = "T2( a int, c string)@python = {* yield (1, 'cdc') *} \n";

		String query = t1 + t2;
		query = query + "SELECT T1.b, T2.c FROM T1 JOIN T2 ON T1.b = T2.c";

		PreparedStatement stmt = conn.prepareStatement(query);
		stmt.execute();
		ResultSet rs = stmt.getResultSet();

		if (rs.next()) {
			Assert.assertEquals("cdc", rs.getString(1));
			Assert.assertEquals("cdc", rs.getString(2));
		}
		rs.close();
		stmt.close();
		conn.commit();
		Assert.assertTrue(true);
	}

	@Test
	public void multipleQueriesInDifferentConnections() throws Exception {
		testJoin();
		testJoin2();
	}

	// @Test
	public void multipleQueriesPerConnection() throws Exception {
		Connection conn = createConnection();
		testJoin1Con(conn);
		testJoin2Con(conn);
		conn.close();
	}

	@Test
	public void testUpdatesOnWrappers() throws Exception {
		Connection conn = createConnection();
		String t1 = "T1(x int, b string )@mock = {* yield (1, 'cdc') *} \n";

		String query = t1 + "SELECT T1.x, T1.b FROM T1";
		PreparedStatement stmt = conn.prepareStatement(query);
		stmt.execute();
		ResultSet rs = stmt.getResultSet();
		rs.close();
		stmt.close();
		conn.commit();
		conn.close();
		Assert.assertTrue(true);

	}

	@Test
	public void testQueryOnWrappers() throws Exception {
		Connection conn = createConnection();
		String t1 = "T1(a int, b string)@mock = (SELECT x, y FROM T) \n";
		String t2 = "T2(a int, b string)@mock = (SELECT x, y FROM T) \n";
		String query = t1 + t2
				+ "SELECT * FROM (SELECT * FROM T1 UNION SELECT * FROM T2) T WHERE b = 'hello' AND a IN (1,2,3)";
		PreparedStatement stmt = conn.prepareStatement(query);
		stmt.execute();
		ResultSet rs = stmt.getResultSet();
		rs.close();
		stmt.close();
		conn.commit();
		conn.close();
		Assert.assertTrue(true);

	}

	@Test
	public void testRollback() throws Exception {
		Connection conn = createConnection();
		conn.setAutoCommit(false);
		String t1 = "T1(x int, b string )@mock = {* yield (1, 'cdc') *} \n";

		String query = t1 + "SELECT T1.x, T1.b FROM T1";
		PreparedStatement stmt = conn.prepareStatement(query);
		stmt.execute();
		ResultSet rs = stmt.getResultSet();
		rs.close();
		stmt.close();
		conn.rollback();
		conn.close();
		Assert.assertTrue(true);

	}

	@Test
	public void testDates() throws Exception {
		Connection conn = createConnection();
		conn.setAutoCommit(false);
		String t1 = "T1(x int, b date )@python = {*yield (1, datetime.date(2007, 12, 5)) *} \n";

		String query = t1 + "SELECT T1.x, T1.b FROM T1";
		PreparedStatement stmt = conn.prepareStatement(query);
		stmt.execute();
		ResultSet rs = stmt.getResultSet();
		while (rs.next()) {
			System.out.println("row " + rs.getRow() + ": " + rs.getString(1) + ", " + rs.getDate(2));
		}
		rs.close();
		stmt.close();
		conn.rollback();
		conn.close();
		Assert.assertTrue(true);

	}

	@Test
	public void testColumnNames() throws Exception {
		Connection conn = createConnection();

		String t1 = "T1(x int, b string )@python = {*\tyield (1, 'cdc')\n\tyield (1, 'cdc') *} \n";

		String query = t1 + "SELECT T1.x as hello, T1.b  FROM T1";
		PreparedStatement stmt = conn.prepareStatement(query);
		stmt.execute();
		ResultSet rs = stmt.getResultSet();
		int size = 0;
		while (rs.next()) {
			size++;
		}
		Assert.assertEquals("hello", rs.getMetaData().getColumnLabel(1));
		Assert.assertEquals(2, size);
		rs.close();
		conn.commit();
		stmt.close();
		conn.close();

	}

	@Test
	public void testDistinct() throws Exception {
		Connection conn = createConnection();

		String t1 = "T1(x int, b string )@python = {*\tyield (1, 'cdc')\n\tyield (1, 'cdc') *} \n";

		String query = t1 + "SELECT DISTINCT T1.x, T1.b  FROM T1";
		PreparedStatement stmt = conn.prepareStatement(query);
		stmt.execute();
		ResultSet rs = stmt.getResultSet();
		int size = 0;
		while (rs.next()) {
			size++;
		}
		Assert.assertEquals(1, size);
		rs.close();
		conn.commit();
		stmt.close();
		conn.close();

	}

	@Test
	public void testAggregates() throws Exception {
		Connection conn = createConnection();
		String t1 = "T1( a int, b int )@python = {*\tyield (1, 1)\n\tyield (1, 1)\n *}\n";
		String query = t1 + "SELECT sum( a ) FROM T1 GROUP BY b";
		PreparedStatement stmt = conn.prepareStatement(query);
		stmt.execute();
		ResultSet rs = stmt.getResultSet();
		rs.next();
		Assert.assertEquals(2, rs.getInt(1));
		rs.close();

		stmt.close();
		conn.commit();
		conn.close();
	}

	@Test
	public void testAggregates2() throws Exception {
		Connection conn = createConnection();
		String t1 = "T1( a int, b int )@python = {*\tyield (1, 150)\n\tyield (1, 150)\n *}\n";
		String query = t1 + "SELECT a, b, sum( a ) FROM T1 WHERE b < 200 GROUP BY b, a";
		PreparedStatement stmt = conn.prepareStatement(query);
		stmt.execute();
		ResultSet rs = stmt.getResultSet();
		rs.next();
		Assert.assertEquals(2, rs.getInt(3));
		rs.close();
		stmt.close();
		conn.commit();
		conn.close();
	}

	@Test
	public void testCountAll() throws Exception {
		Connection conn = createConnection();
		String t1 = "T1( a int, b int )@python = {*\tyield (1, 150)\n\tyield (1, 150)\n *}\n";
		String query = t1 + "SELECT a, b, count( * ) FROM T1 WHERE b < 200 GROUP BY b, a";
		PreparedStatement stmt = conn.prepareStatement(query);
		stmt.execute();
		ResultSet rs = stmt.getResultSet();
		rs.next();
		Assert.assertEquals(2, rs.getInt(3));
		rs.close();
		stmt.close();
		conn.commit();
		conn.close();
	}

	@Test
	public void testHaving() throws Exception {
		Connection conn = createConnection();
		String t1 = "T1( a int, b int)@python = {*\tyield (1, 150)\n\tyield (1, 150)\n *}\n";
		String query = t1 + "SELECT a, b, count( * ) FROM T1 WHERE b < 200 GROUP BY b, a HAVING a = 1";
		PreparedStatement stmt = conn.prepareStatement(query);
		stmt.execute();
		ResultSet rs = stmt.getResultSet();
		rs.next();
		Assert.assertEquals(2, rs.getInt(3));
		rs.close();
		stmt.close();
		conn.commit();
		conn.close();
	}

	@Test
	public void testArithmeticOperations() throws Exception {
		Connection conn = createConnection();
		String t1 = "T1( a int, b int)@python = {*\tyield (1, 150)\n\tyield (1, 150)\n *}\n";
		String query = t1 + "SELECT a, b, a+b  FROM T1 WHERE b < 200 GROUP BY b, a";
		PreparedStatement stmt = conn.prepareStatement(query);
		stmt.execute();
		ResultSet rs = stmt.getResultSet();
		rs.next();
		Assert.assertEquals(151, rs.getInt(3));
		rs.close();
		stmt.close();
		conn.commit();
		conn.close();
	}

	@Test
	public void testArithmeticOperationsFromGroupBy() throws Exception {
		Connection conn = createConnection();
		String t1 = "T1( a int, b int)@python = {*\tyield (1, 150)\n\tyield (1, 150)\n *}\n";
		String query = t1 + "SELECT a+b  FROM T1 WHERE b < 200 GROUP BY a+b";
		PreparedStatement stmt = conn.prepareStatement(query);
		stmt.execute();
		ResultSet rs = stmt.getResultSet();
		rs.next();
		Assert.assertEquals(151, rs.getInt(1));
		rs.close();
		stmt.close();
		conn.commit();
		conn.close();
	}

	@Test
	public void testComplexAggrAndGroupBy() throws Exception {
		Connection conn = createConnection();
		String t1 = "T1( a int, b int)@python = {*\tyield (1, 150)\n\tyield (1, 150)\n *}\n";
		String query = t1
				+ "SELECT a * b, max(a + b), count(*)  FROM T1 WHERE b < 200 GROUP BY a*b HAVING count(*) > 0";
		PreparedStatement stmt = conn.prepareStatement(query);
		stmt.execute();
		ResultSet rs = stmt.getResultSet();
		rs.next();
		Assert.assertEquals(150, rs.getInt(1));
		Assert.assertEquals(151, rs.getInt(2));
		Assert.assertEquals(2, rs.getInt(3));
		rs.close();
		stmt.close();
		conn.commit();
		conn.close();
	}

	@Test
	public void testAggregationWihoutGroupBy() throws Exception {
		Connection conn = createConnection();
		String t1 = "T1( a int, b int)@python = {*\tyield (1, 150)\n\tyield (1, 150)\n *}\n";
		String query = t1 + "SELECT sum(a), count(b), count(*)  FROM T1 HAVING max(b) > 100";
		PreparedStatement stmt = conn.prepareStatement(query);
		stmt.execute();
		ResultSet rs = stmt.getResultSet();
		rs.next();
		Assert.assertEquals(2, rs.getInt(1));
		Assert.assertEquals(2, rs.getInt(2));
		Assert.assertEquals(2, rs.getInt(3));
		rs.close();
		stmt.close();
		conn.commit();
		conn.close();
	}

	public void testCount() throws Exception {
		Connection conn = createConnection();
		String t1 = "T1( a int, b int)@python = {*\tyield (1, 150)\n\tyield (1, 150)\n *}\n";
		String query = t1 + "SELECT count(*)  FROM T1";
		PreparedStatement stmt = conn.prepareStatement(query);
		stmt.execute();
		ResultSet rs = stmt.getResultSet();
		rs.next();
		Assert.assertEquals(2, rs.getInt(1));
		rs.close();
		stmt.close();
		conn.commit();
		conn.close();
	}

	@Test
	public void testWhenThen() throws Exception {
		Connection conn = createConnection();
		String t1 = "T1( a int, b int)@python = {*\tyield (1, 150)\n\tyield (1, 150)\n *}\n";
		String query = t1 + "SELECT CASE a WHEN 0 THEN a ELSE b END as X FROM T1";
		PreparedStatement stmt = conn.prepareStatement(query);
		stmt.execute();
		ResultSet rs = stmt.getResultSet();
		rs.next();
		Assert.assertEquals(150, rs.getInt(1));
		rs.close();
		stmt.close();
		conn.commit();
		conn.close();
	}

	@Test
	public void testWhenThenUsingNulls() throws Exception {
		Connection conn = createConnection();
		String t1 = "T1( a int, b int)@python = {*\tyield (1, 150)\n\tyield (1, 150)\n *}\n";
		String query = t1 + "SELECT CASE WHEN a IS NULL THEN a ELSE b END as X FROM T1";
		PreparedStatement stmt = conn.prepareStatement(query);
		stmt.execute();
		ResultSet rs = stmt.getResultSet();
		rs.next();
		Assert.assertEquals(150, rs.getInt(1));
		rs.close();
		stmt.close();
		conn.commit();
		conn.close();
	}

	@Test
	public void testMultipleWhenThen() throws Exception {
		Connection conn = createConnection();
		String t1 = "T1( a int, b int, c int)@python = {*\tyield (1, 150, 150)\n\tyield (1, 150)\n *}\n";
		String query = t1 + "SELECT CASE WHEN c IS NULL THEN a ELSE b END as X, "
				+ " CASE a WHEN 0 THEN 'zero'  WHEN -1 THEN 'negative' WHEN 1 THEN 'positive' END as Y FROM T1";
		PreparedStatement stmt = conn.prepareStatement(query);
		stmt.execute();
		ResultSet rs = stmt.getResultSet();
		rs.next();
		Assert.assertEquals(150, rs.getInt(1));
		Assert.assertEquals("positive", rs.getString(2));
		rs.close();
		stmt.close();
		conn.commit();
		conn.close();
	}

	@Test
	public void testQueryParameters() throws Exception {
		Connection conn = createConnection();
		String t1 = "T1( a int, b int, c string WITHPARAMS x string)@python = {*\tyield (1, 150, x)\n\tyield (1, 150, x)\n *}\n";
		String query = t1 + "SELECT * FROM T1( $1 )";
		PreparedStatement stmt = conn.prepareStatement(query);
		stmt.setString(1, "foo");
		stmt.execute();
		ResultSet rs = stmt.getResultSet();
		rs.next();
		Assert.assertEquals(1, rs.getInt(1));
		Assert.assertEquals(150, rs.getInt(2));
		Assert.assertEquals("foo", rs.getString(3));
		rs.close();
		stmt.close();
		conn.commit();
		conn.close();
	}

	@Test
	public void testQueryParametersWithDifferentTypes() throws Exception {
		Connection conn = createConnection();
		String t1 = "T1( a int, b int, c string WITHPARAMS x string, y int)@python = {*\tyield (1, 150, x)\n\tyield (1, 150, x)\n *}\n";
		String query = t1 + "SELECT * FROM T1( $1, $2 )";
		PreparedStatement stmt = conn.prepareStatement(query);
		stmt.setString(1, "foo");
		stmt.setInt(2, 0);
		stmt.execute();
		ResultSet rs = stmt.getResultSet();
		rs.next();
		Assert.assertEquals(1, rs.getInt(1));
		Assert.assertEquals(150, rs.getInt(2));
		Assert.assertEquals("foo", rs.getString(3));
		rs.close();
		stmt.close();
		conn.commit();
		conn.close();
	}

	@Test
	public void testQueryParametersInNestedQueries() throws Exception {
		Connection conn = createConnection();
		String t1 = "T1( a int, b int, c string WITHPARAMS x string)@python = {*\tyield (1, 150, x)\n\tyield (1, 150, x)\n *}\n";
		String t2 = "T2( a int, b int, c string WITHPARAMS x string) = (SELECT * FROM T1( $x ))\n";
		String query = t1 + t2 + "SELECT * FROM T2( $1 )";
		PreparedStatement stmt = conn.prepareStatement(query);
		stmt.setString(1, "foo");
		stmt.execute();
		ResultSet rs = stmt.getResultSet();
		rs.next();
		Assert.assertEquals(1, rs.getInt(1));
		Assert.assertEquals(150, rs.getInt(2));
		Assert.assertEquals("foo", rs.getString(3));
		rs.close();
		stmt.close();
		conn.commit();
		conn.close();
	}

	@Test
	public void testQueryParametersInNestedQueries2() throws Exception {
		Connection conn = createConnection();
		String t1 = "T1( a int, b int, c string WITHPARAMS x string, y string)@python = {*\tyield (1, 150, y)\n\tyield (1, 150, x)\n *}\n";
		String t2 = "T2( a int, b int, c string WITHPARAMS x string) = (SELECT * FROM T1( $x, $x ))\n";
		String query = t1 + t2 + "SELECT * FROM T2( $1 )";
		PreparedStatement stmt = conn.prepareStatement(query);
		stmt.setString(1, "foo");
		stmt.execute();
		ResultSet rs = stmt.getResultSet();
		rs.next();
		Assert.assertEquals(1, rs.getInt(1));
		Assert.assertEquals(150, rs.getInt(2));
		Assert.assertEquals("foo", rs.getString(3));
		rs.next();
		Assert.assertEquals(1, rs.getInt(1));
		Assert.assertEquals(150, rs.getInt(2));
		Assert.assertEquals("foo", rs.getString(3));
		rs.close();
		stmt.close();
		conn.commit();
		conn.close();
	}

	@Test
	public void testQueryUnion() throws Exception {
		Connection conn = createConnection();
		String t1 = "T1( a int, b int, c string)@python = {*\tyield (1, 150, 'cdc')\n\tyield (1, 150, 'abc')\n *}\n";
		String t2 = "T2( a int, b int, c string)@python = {*\tyield (1, 150, 'cdc')\n\tyield (1, 150, 'abc')\n *}\n";
		String query = t1 + t2 + "SELECT * FROM T1 UNION SELECT * FROM T2";
		PreparedStatement stmt = conn.prepareStatement(query);
		stmt.execute();
		ResultSet rs = stmt.getResultSet();
		rs.next();
		Assert.assertEquals(1, rs.getInt(1));
		Assert.assertEquals(150, rs.getInt(2));
		Assert.assertEquals("abc", rs.getString(3));
		rs.next();
		Assert.assertEquals(1, rs.getInt(1));
		Assert.assertEquals(150, rs.getInt(2));
		Assert.assertEquals("cdc", rs.getString(3));
		Assert.assertFalse(rs.next());
		rs.close();
		stmt.close();
		conn.commit();
		conn.close();
	}

	@Test
	public void testQueryUnionAll() throws Exception {
		Connection conn = createConnection();
		String t1 = "T1( a int, b int, c string)@python = {*\tyield (1, 150, 'cdc')\n\tyield (1, 150, 'abc')\n *}\n";
		String t2 = "T2( a int, b int, c string)@python = {*\tyield (1, 150, 'cdc')\n\tyield (1, 150, 'abc')\n *}\n";
		String query = t1 + t2 + "SELECT * FROM T1 UNION ALL SELECT * FROM T2";
		PreparedStatement stmt = conn.prepareStatement(query);
		stmt.execute();
		ResultSet rs = stmt.getResultSet();
		int count = 0;
		while (rs.next()) {
			count++;
		}
		rs.close();
		stmt.close();
		conn.commit();
		conn.close();
		Assert.assertEquals(4, count);
	}

	@Test
	public void testSortBy() throws Exception {
		Connection conn = createConnection();
		String t1 = "T1( a int, b int, c string)@python = {*\tyield (1, 150, 'cdc')\n\tyield (1, 150, 'abc')\n *}\n";
		String query = t1 + "SELECT * FROM T1 ORDER BY c asc";
		PreparedStatement stmt = conn.prepareStatement(query);
		stmt.execute();
		ResultSet rs = stmt.getResultSet();
		rs.next();
		Assert.assertEquals(1, rs.getInt(1));
		Assert.assertEquals(150, rs.getInt(2));
		Assert.assertEquals("abc", rs.getString(3));
		rs.next();
		Assert.assertEquals(1, rs.getInt(1));
		Assert.assertEquals(150, rs.getInt(2));
		Assert.assertEquals("cdc", rs.getString(3));
		rs.close();
		stmt.close();
		conn.commit();
		conn.close();
	}

	@Test
	public void testSortByLimit() throws Exception {
		Connection conn = createConnection();
		String t1 = "T1( a int, b int, c string)@python = {*\tyield (1, 150, 'cdc')\n\tyield (1, 150, 'abc')\n *}\n";
		String query = t1 + "SELECT * FROM T1 ORDER BY c asc LIMIT 1";
		PreparedStatement stmt = conn.prepareStatement(query);
		stmt.execute();
		ResultSet rs = stmt.getResultSet();
		rs.next();
		Assert.assertEquals(1, rs.getInt(1));
		Assert.assertEquals(150, rs.getInt(2));
		Assert.assertEquals("abc", rs.getString(3));

		Assert.assertEquals(false, rs.next());

		rs.close();
		stmt.close();
		conn.commit();
		conn.close();
	}

	@Test
	public void testExecute() throws Exception {
		Connection conn = createConnection();
		String a1 = "A2(a int, b int, c string)@python = {*\tyield (1, 150, 'cdc')\n\tyield (1, 150, 'abc')\n*} \n";
		String query = a1 + "EXECUTE A2()";
		PreparedStatement stmt = conn.prepareStatement(query);
		stmt.execute();
		stmt.close();
		conn.commit();
		conn.close();
	}

	@Test
	public void testExecuteWithParams() throws Exception {
		Connection conn = createConnection();
		String a1 = "A2(WITHPARAMS value string, value2 string)@python = {*\tyield (1, 150, 'cdc')\n\tyield (1, 150, 'abc')\n*} \n";
		String query = a1 + "EXECUTE A2($1, $2)";
		PreparedStatement stmt = conn.prepareStatement(query);
		stmt.setString(1, "foo");
		stmt.setString(2, "foo");
		stmt.execute();
		stmt.close();
		conn.commit();
		conn.close();
	}

	@Test
	public void testExecuteWithParams2() throws Exception {
		Connection conn = createConnection();
		String a1 = "A2(WITHPARAMS value string, value2 int)@python = {*\tyield (1, 150, 'cdc')\n\tyield (1, 150, 'abc')\n*} \n";
		String query = a1 + "EXECUTE A2($1, $2)";
		PreparedStatement stmt = conn.prepareStatement(query);
		stmt.setString(1, "foo");
		stmt.setInt(2, 0);
		stmt.execute();
		stmt.close();
		conn.commit();
		conn.close();
	}

	@Test
	public void testMultipleExecuteWithParams() throws Exception {
		Connection conn = createConnection();
		String a1 = "A2(WITHPARAMS value string, value2 int)@python = {* *} \n";
		String query = a1 + "EXECUTE A2($1, $2) EXECUTE A2($3, $4)";
		PreparedStatement stmt = conn.prepareStatement(query);
		stmt.setString(1, "foo");
		stmt.setInt(2, 0);
		stmt.setString(3, "foo");
		stmt.setInt(4, 0);
		stmt.execute();
		stmt.close();
		conn.commit();
		conn.close();
	}

	@Test
	public void testJoinColumnNameProblems() throws Exception {
		Connection conn = createConnection();
		String t1 = "NewScenarioResults( cntr_id int, chargeamount double, billcycle_id int, whifsc_id string)@python"
				+ "= {*\tyield(5, 2.0, 1, 'abc')\n*}\n";

		String t2 = "InvoicesWithoutS( allContract string, cntr_id int, scenario string,chargeamount double)@python"
				+ "= {*\tyield('abc', 5, 'abc', 2.0)\n*}\n";

		String query = t1 + t2 + " SELECT a.cntr_id, a.chargeamount, a.whifsc_id "
				+ " FROM NewScenarioResults a JOIN InvoicesWithoutS b" + " ON a.cntr_id = b.cntr_id ";

		PreparedStatement stmt = conn.prepareStatement(query);
		stmt.execute();
		stmt.close();
		conn.commit();
		conn.close();
	}

	// @Test
	public void testSelfJoinColumnNameProblems() throws Exception {
		Connection conn = createConnection();
		String t1 = "NewScenarioResults( cntr_id int, chargeamount double, billcycle_id int, whifsc_id string)@python"
				+ "= {*\tyield(5, 2.0, 1, 'abc')\n*}\n";

		String query = t1 + " SELECT a.cntr_id, a.chargeamount, a.whifsc_id "
				+ " FROM NewScenarioResults a JOIN NewScenarioResults b" + " ON a.cntr_id = b.cntr_id ";

		// APP.CTXT1444063760306_NEWSCENARIORESULTS.R0
		PreparedStatement stmt = conn.prepareStatement(query);
		stmt.execute();
		stmt.close();
		conn.commit();
		conn.close();
	}

	@Test
	public void testParsingProblem() throws Exception {
		Connection conn = createConnection();
		String t1 = "ContractsWithS( allContract string, cntr_id int, scenario string,chargeamount double WITHPARAMS scenario string, bc1 string, bc2 string ) @python= {* *}\n";

		String t2 = "NewScenarioResults( cntr_id int, chargeamount double, billcycle_id int, whifsc_id string  WITHPARAMS newscenario string, bc1 int, bc2 int) @python={* *}\n";

		String t3 = "InvoicesWithoutS( allContract string, cntr_id int, scenario string,chargeamount double WITHPARAMS scenario string, bc1 string, bc2 string ) @python= {* *}\n";

		String t4 = "T2 (cntr_id int, chargeamount double,scenario string) = (SELECT newinv.cntr_id as cntr,  newinv.chargeamount as chargeamount, newinv.whifsc_id as scenario "
				+

		"FROM  NewScenarioResults('2',20140101,20140601) newinv JOIN  ContractsWithS('1','Wed Jan 01 00:00:00 EET 2014','Sun Jun 01 00:00:00 EEST 2014') c ON newinv.cntr_id = c.cntr_id UNION  SELECT InvoicesWithoutS.cntr_id as cntr, InvoicesWithoutS.chargeamount  as chargeamount, InvoicesWithoutS.scenario as scenario FROM InvoicesWithoutS('1','Wed Jan 01 00:00:00 EET 2014','Sun Jun 01 00:00:00 EEST 2014') )\n";

		String query = t1 + t2 + t3 + t4 + "SELECT SUM(T2.chargeamount) FROM T2";
		PreparedStatement stmt = conn.prepareStatement(query);
		stmt.execute();
		stmt.close();
		conn.commit();
		conn.close();
	}

	@Test
	public void testMultipleStmtExecutionsWithParams() throws Exception {
		Connection conn = createConnection();
		String a1 = "A2(WITHPARAMS a string, b int)@python = {* *} \n";
		String query = a1 + "EXECUTE A2($1, $2)";
		PreparedStatement stmt = conn.prepareStatement(query);
		stmt.setString(1, "foo");
		stmt.setInt(2, 0);

		stmt.execute();

		stmt.getResultSet().close();
		conn.commit();

		stmt.setString(1, "foo");
		stmt.setInt(2, 0);
		stmt.execute();
		stmt.close();

		conn.commit();
		conn.close();
	}

	@Test
	public void testMultipleCommits() throws Exception {
		Connection conn = createConnection();
		testJoin1Con(conn);
		conn.commit();
		testJoin2Con(conn);
		conn.commit();
		conn.close();
	}

	@Test
	public void testBatchUpdatesInPython() throws Exception {
		Connection conn = createConnection();

		String t1 = "t1(WITHPARAMS value2 int, value string)@python = {* *} \n";

		String a1 = "a1(value int, bar string)@python = {*\tyield (150, 'cdc')\n\tyield (150, 'abc')\n*} \n";

		String a2 = "a2(REFERENCING T1, A1)@python = {*\tsp = CloudMdsQL.a1()\n\twhile sp.next():\n\t\tCloudMdsQL.t1(sp.getInt(1), sp.getString(2))\n\tsp.close()\n*}\n";

		String query = t1 + a1 + a2 + "EXECUTE a2";
		PreparedStatement stmt = conn.prepareStatement(query);

		stmt.execute();
		stmt.close();
		conn.commit();
		conn.close();
	}

	public void testConcurrentQueries() throws Exception {
		final Connection con = createConnection();

		class ThreadA extends Thread {
			@Override
			public void run() {
				try {
					testJoin1Con(con);
				} catch (Exception e) {
					e.printStackTrace();
					Assert.assertTrue(false);
				}
			}
		}
		;

		class ThreadB extends Thread {
			@Override
			public void run() {
				try {
					testJoin2Con(con);
				} catch (Exception e) {
					e.printStackTrace();
					Assert.assertTrue(false);
				}
			}
		}
		;

		ThreadA a1 = new ThreadA();
		ThreadA a2 = new ThreadA();

		ThreadB b1 = new ThreadB();
		ThreadB b2 = new ThreadB();

		a1.start();
		a2.start();
		b1.start();
		b2.start();

		while (a1.isAlive() && a2.isAlive() && b1.isAlive() && b2.isAlive()) {
			Thread.sleep(100);
		}
		con.close();
	}

	@Test
	public void testExecuteQueryJDBCMethod() throws Exception {

		Connection conn = createConnection();

		String t1 = "T1( x long, y string )@python = {* yield (1L, 'abc') *}";
		String query = "SELECT T1.x, T1.y FROM T1 WHERE T1.x = 1\n" + t1;

		Statement stmt = conn.createStatement();

		ResultSet rs = stmt.executeQuery(query);

		while (rs.next()) {
			System.out.println("row " + rs.getRow() + ": " + rs.getInt(1) + ", " + rs.getString(2));
		}

		rs.close();
		stmt.close();
		conn.commit();
		conn.close();
		Assert.assertTrue(true);
	}

	@Test
	public void testDynamicExecutionWithDifferentParams() throws Exception {
		Connection conn = createConnection();
		String taula1 = "taula1(missatge string WITHPARAMS nom string)@python = {* yield(nom) *} \n";
		String taula2 = "taula2(missatge2 string REFERENCING taula1)@python = {*\tsp = CloudMdsQL.taula1('Raquel')\n\tif sp.next():\n\t\tyield(sp.getString(1))\n\tsp.close()\n\tsp = CloudMdsQL.taula1('Mike')\n\tif sp.next():\n\t\tyield(sp.getString(1))\n\tsp.close()\n*} \n";
		String query = taula1 + taula2 + "select missatge2 from taula2";
		PreparedStatement stmt = conn.prepareStatement(query);

		stmt.execute();
		ResultSet rs = stmt.getResultSet();
		try {
			rs.next();
			Assert.assertEquals("Raquel", rs.getString(1));

			rs.next();
			Assert.assertEquals("Mike", rs.getString(1));
		} finally {
			stmt.close();
			conn.commit();
			conn.close();
		}
		Assert.assertTrue(true);
	}

	@Test
	public void testNamedTablesAsScalarFunctions() throws Exception {
		Connection conn = createConnection();
		String t0 = "T0(a string WITHPARAMS x string)@python = {* yield (x+'hello') *} \n";
		String t1 = "T1(a string, b int)@python = {*\tyield ('abc', 1)\n\tyield ('cde', 2)*} \n";
		String t2 = "T2(a string, b int) = ( SELECT T0(a), b FROM T1 ) \n";
		String query = t0 + t1 + t2 + "SELECT * FROM T2";
		PreparedStatement stmt = conn.prepareStatement(query);

		stmt.execute();
		ResultSet rs = stmt.getResultSet();
		try {
			rs.next();
			Assert.assertEquals("abchello", rs.getString(1));

			rs.next();
			Assert.assertEquals("cdehello", rs.getString(1));
		} finally {
			stmt.close();
			conn.commit();
			conn.close();
		}
		Assert.assertTrue(true);

	}

	@Test
	public void testInNamedTableFunction() throws Exception {
		Connection conn = createConnection();
		String t0 = "T0(a string WITHPARAMS x string)@python = {* yield ('abc') *}\n";
		String t1 = "T1(a string)@python = {* yield ('abchello') *}\n";
		String t2 = "T2(a string) = ( SELECT * FROM T1 WHERE a IN T0(a) )\n";
		String query = t0 + t1 + t2 + "SELECT * FROM T2";
		PreparedStatement stmt = conn.prepareStatement(query);

		stmt.execute();
		ResultSet rs = stmt.getResultSet();
		try {
			Assert.assertFalse(rs.next());
		} finally {
			stmt.close();
			conn.commit();
			conn.close();
		}
		Assert.assertTrue(true);

	}

	@Test
	public void testArrayValuedScalars() throws Exception {
		Connection conn = createConnection();
		String t0 = "T0(a string, b int WITHPARAMS c string)@python = {* yield (c, 1) *}\n";
		String t1 = "T1(a string, b int)@python = {* yield ('abc', 2) *}";
		String query = t0 + t1 + "SELECT T0(a)[0], T0(a)[1], b FROM T1";
		PreparedStatement stmt = conn.prepareStatement(query);

		stmt.execute();
		ResultSet rs = stmt.getResultSet();
		try {
			Assert.assertTrue(rs.next());
		} finally {
			stmt.close();
			conn.commit();
			conn.close();
		}
		Assert.assertTrue(true);

	}
	
	@Test
	public void testArrayValuedScalarsWithFloats() throws Exception {
		Connection conn = createConnection();
		String t0 = "T0(a float, b int WITHPARAMS c float)@python = {* yield (c, 1) *}\n";
		String t1 = "T1(a float, b int)@python = {* yield (0.125, 2) *}";
		String query = t0 + t1 + "SELECT T0(a)[0], T0(a)[1], b FROM T1";
		PreparedStatement stmt = conn.prepareStatement(query);

		stmt.execute();
		ResultSet rs = stmt.getResultSet();
		try {
			Assert.assertTrue(rs.next());
		} finally {
			stmt.close();
			conn.commit();
			conn.close();
		}
		Assert.assertTrue(true);

	}

	@Test
	public void testArraysAsParams() throws Exception {
		Connection conn = createConnection();
		String t1 = "T1(a int, b int)@python = {* yield (2, 1) *}\n";
		String t2 = "T2(a ARRAY WITHPARAMS value ARRAY) = (SELECT [$value[0], a+b] FROM T1)\n";
		String query = t1 + t2 + "SELECT a[0], a[1] FROM T2([1,2,3])";
		PreparedStatement stmt = conn.prepareStatement(query);

		stmt.execute();
		ResultSet rs = stmt.getResultSet();
		try {
			Assert.assertTrue(rs.next());
		} finally {
			stmt.close();
			conn.commit();
			conn.close();
		}
		Assert.assertTrue(true);
	}

	@Test
	public void testSharedTxId() throws Exception {
		Connection conn = createConnection();
		String t1 = "A(tid long)@python = {* yield(ctx.getId()) *} \n";
		String query = t1 + "SELECT * FROM A";
		PreparedStatement stmt = conn.prepareStatement(query);
		stmt.execute();

		ResultSet rs = stmt.getResultSet();
		rs.next();
		Long txId = rs.getLong(1);
		rs.close();
		stmt.close();
		t1 = "A0(WITHPARAMS x int, y string)@mock = ( INSERT INTO tbl(a, b, c) VALUES ($x, $y, 'c')) \n";
		conn = createConnection((long) txId);
		query = t1 + "EXECUTE A0(0, 'hello')";
		stmt = conn.prepareStatement(query);
		stmt.execute();
		stmt.close();

		conn.commit();
		conn.close();
		Assert.assertTrue(true);

	}

	@Test
	public void testExecuteWithMultipleStmtExecutions() throws Exception {
		Connection conn = createConnection();
		String a1 = "A2(WITHPARAMS value string, value2 int)@python = {* *} \n";
		String query = a1 + "EXECUTE A2($1, $2)";
		PreparedStatement stmt = conn.prepareStatement(query);

		for (int i = 0; i < 10; i++) {
			stmt.setString(1, "foo");
			stmt.setInt(2, i);
			stmt.execute();
			conn.commit();
		}

		stmt.close();
		conn.commit();
		conn.close();
	}

	@Test
	public void testNamedActions() throws Exception {
		Connection conn = createConnection();
		String t1 = "A0(WITHPARAMS x int, y string)@mock = ( INSERT INTO tbl(a, b, c) VALUES ($x, $y, 'c')) \n";

		String query = t1 + "EXECUTE A0(0, 'hello')";
		PreparedStatement stmt = conn.prepareStatement(query);
		stmt.execute();
		stmt.close();
		conn.commit();
		conn.close();
		Assert.assertTrue(true);

	}

	@Test
	public void testJoinNestedSelects() throws Exception {
		Connection conn = createConnection();
		String t1 = "T1( a int,  c string )@mock = ( SELECT t1.a,  t2.c FROM T1 JOIN (SELECT b, c FROM T1 WHERE a = 0) t2 ON t1.a = t2.b) \n";

		String query = t1 + "SELECT t1.*, t2.c FROM T1 JOIN (SELECT * FROM T1 WHERE a = 0) t2 ON t1.a = t2.a";
		PreparedStatement stmt = conn.prepareStatement(query);
		stmt.execute();
		stmt.close();
		conn.commit();
		conn.close();
		Assert.assertTrue(true);
	}

	@Test
	public void testBugOnTablesBlockingFromPython() throws Exception {
		Connection conn = createConnection();
		String classifies = "classifies(communityid string)@python = {*\tyield('1')\n\tyield('2')\n*}\n";
		String classifiesdos = "classifiesdos(communityid string)@python = {*\tyield('1')\n\tyield('2')\n*}\n";
		String influencers = "influencers(communityid string REFERENCING classifies)@python = {*\tsp = CloudMdsQL.classifies()\n\twhile sp.next():\n\t\tidentcommunity = sp.getString(1)\n\t\tyield(identcommunity)\n\tsp.close()\n*}\n";
		String communityinfluencers = "communityinfluencers(communityid string)= ( SELECT influencers.communityid FROM influencers JOIN classifies ON classifies.communityid = influencers.communityid)\n";
		String query = classifies + classifiesdos + influencers + communityinfluencers
				+ "select communityid from communityinfluencers";

		PreparedStatement stmt = conn.prepareStatement(query);
		stmt.execute();

		ResultSet rs = stmt.getResultSet();
		int rows = 0;
		while (rs.next()) {
			rows++;
		}
		Assert.assertEquals(2, rows);
		stmt.close();
		conn.commit();
		conn.close();
		Assert.assertTrue(true);
	}

	@Test
	public void testInQuery() throws Exception {
		Connection conn = createConnection();
		String t1 = "T1(a string, b string)@python = {* yield ('abc', 'z') *}\n";
		String query = t1 + "SELECT * FROM T1 WHERE b IN ('a', 'b', 'c') OR a IN (SELECT a FROM T1 WHERE b = 'z')";
		PreparedStatement stmt = conn.prepareStatement(query);
		stmt.execute();
		ResultSet rs = stmt.getResultSet();
		int rows = 0;
		while (rs.next()) {
			rows++;
			rs.getString(1);
		}
		Assert.assertEquals(1, rows);
		stmt.close();
		conn.commit();
		conn.close();
		Assert.assertTrue(true);
	}

	private void initCustomTypes(Connection conn) throws Exception {
		// deleteCustomTypes(conn);
		Statement stmtPrev = null;
		try {
			stmtPrev = conn.createStatement();

			stmtPrev.execute("CREATE TYPE MAP EXTERNAL NAME '" + CloudMdSQLMap.class.getName() + "' LANGUAGE JAVA");

			stmtPrev.close();

			stmtPrev = conn.createStatement();

			stmtPrev.execute(
					"CREATE TYPE MapEntry EXTERNAL NAME '" + CloudMdSQLMapEntry.class.getName() + "' LANGUAGE JAVA");

			stmtPrev.close();

			stmtPrev = conn.createStatement();
			stmtPrev.execute("CREATE TYPE ARRAY EXTERNAL NAME '" + CloudMdSQLArray.class.getName() + "' LANGUAGE JAVA");

			stmtPrev.close();
			conn.commit();
		} catch (Exception e) {
			System.out.println("hello");
		} finally {
			if (stmtPrev != null && !stmtPrev.isClosed()) {
				stmtPrev.close();
			}
			conn.close();
		}
	}

	@Test
	public void testArrays() throws Exception {
		Connection conn = createConnection();

		String t1 = "T1(a int, b int)@python = {* yield (2, 1) *}\n";
		String query = t1 + "SELECT [1, a+b] FROM T1";
		PreparedStatement stmt = conn.prepareStatement(query);
		stmt.execute();
		ResultSet rs = stmt.getResultSet();
		int rows = 0;
		while (rs.next()) {
			rows++;
			ArrayList o = (ArrayList) rs.getObject(1);
			Assert.assertEquals(2, o.size());
		}
		Assert.assertEquals(1, rows);
		stmt.close();
		conn.commit();

		conn.close();
		Assert.assertTrue(true);
	}

	@Test
	public void testArrays2() throws Exception {
		Connection conn = createConnection();

		String t1 = "T1(a int, b int)@python = {* yield (2, 1) *}\n";
		String t2 = "T2(a array) = (SELECT [1, a+b] FROM T1)\n";
		String query = t1 + t2 + "SELECT a[0], a[1] FROM T2";
		PreparedStatement stmt = conn.prepareStatement(query);
		stmt.execute();
		ResultSet rs = stmt.getResultSet();
		int rows = 0;
		while (rs.next()) {
			rows++;
		}
		Assert.assertEquals(1, rows);
		stmt.close();
		conn.commit();

		conn.close();
		Assert.assertTrue(true);
	}

	@Test
	public void testDictionaries() throws Exception {
		Connection conn = createConnection();

		String t1 = "T1(a int, b int)@python = {* yield (2, 1) *}\n";
		String t2 = "T2(b dictionary) = (SELECT {'sum': a+b, 'diff': a-b} FROM T1)\n";
		String query = t1 + t2 + "SELECT b['sum'], b['diff'] FROM T2";
		PreparedStatement stmt = conn.prepareStatement(query);
		stmt.execute();
		ResultSet rs = stmt.getResultSet();
		int rows = 0;
		while (rs.next()) {
			rows++;
		}
		Assert.assertEquals(1, rows);
		stmt.close();
		conn.commit();
		conn.close();
		Assert.assertTrue(true);
	}

	@Test
	public void testArraysAndDictionaries() throws Exception {
		Connection conn = createConnection();

		String t1 = "T1(a int, b int)@python = {* yield (2, 1) *}\n";
		String t2 = "T2(a array, b dictionary) = (SELECT [1, a+b, {'sum': a+b, 'diff': a-b}], {'v': [a, b], 'mult': a*b, 'div': a/b}  FROM T1)\n";
		String query = t1 + t2 + "SELECT a[0], a[2]['sum'], b['mult'], b['v'][1] FROM T2";
		PreparedStatement stmt = conn.prepareStatement(query);
		stmt.execute();
		ResultSet rs = stmt.getResultSet();
		int rows = 0;
		while (rs.next()) {
			rows++;
		}
		Assert.assertEquals(1, rows);
		stmt.close();
		conn.commit();
		conn.close();
		Assert.assertTrue(true);
	}

	@Test
	public void testArbitraryFunctions() throws Exception {
		Connection conn = createConnection();
		String t1 = "T1(a string, b timestamp)@python = {* *}\n";
		String query = t1 + "SELECT row_number(), timestampadd('minute', 5, b) FROM T1";
		PreparedStatement stmt = conn.prepareStatement(query);
		stmt.execute();
		ResultSet rs = stmt.getResultSet();

		Assert.assertFalse(rs.next());
		stmt.close();
		conn.commit();
		conn.close();
		Assert.assertTrue(true);
	}

	@Test
	public void testError() throws Exception {
		String t1 = "T2( cntr_id long, scenario string,chargeamount double "
				+ "WITHPARAMS scenario string, bc1 long, bc2 long )@python = {* *}\n";

		String t4 = "T1 (cntr_id long, chargeamount double,scenario string WITHPARAMS  prevScenario string, altScenario string, bc1 long, bc2 long)"
				+ "= (SELECT cntr_id,  chargeamount , scenario" + " FROM  T2($prevScenario,$bc1,$bc2))\n";
		// bc1 => (? num 2) pero en el contexto anterior, el $2 es un string

		String query = t1 + t4 + "SELECT SUM(T1.chargeamount) as totcharge FROM T1('a','b',1,2)";
		Connection conn = createConnection();
		PreparedStatement stmt = conn.prepareStatement(query);

		// stmt.setString(1, "a");
		// stmt.setString(2, "b");
		// stmt.setLong(3, Long.parseLong("1"));
		// stmt.setLong(4, Long.parseLong("2"));

		stmt.execute();
		ResultSet rs = stmt.getResultSet();
		Assert.assertTrue(rs.next());
		stmt.close();
		conn.commit();
		conn.close();
	}

	@Test
	public void testDates2() throws Exception {
		String t1 = "T1( a int, b date )@python = {* *}\n";
		String query = t1 + "SELECT T1.a, T1.b FROM T1";
		Connection conn = createConnection();
		PreparedStatement stmt = conn.prepareStatement(query);
		stmt.execute();
		ResultSet rs = stmt.getResultSet();
		Assert.assertFalse(rs.next());
		stmt.close();
		conn.commit();
		conn.close();

	}

	@Test
	public void testParticularQuery() throws Exception {
		String t1 = "CQET1(counter int, b int)@python = {* *}\n";
		String query = t1 + "SELECT CQET1.counter FROM CQET1 CQET1";
		Connection conn = createConnection();
		PreparedStatement stmt = conn.prepareStatement(query);
		stmt.execute();
		ResultSet rs = stmt.getResultSet();
		Assert.assertFalse(rs.next());
		stmt.close();
		conn.commit();
		conn.close();
	}

	@Test
	public void testReuseStatementAfterCommit() throws Exception {
		Connection conn = createConnection();
		String t1 = "T1( a int, b int, c string WITHPARAMS x string)@python = {*\tyield (1, 150, x)\n\tyield (1, 150, x)\n *}\n";
		String query = t1 + "SELECT * FROM T1( $1 )";
		PreparedStatement stmt = conn.prepareStatement(query);
		stmt.setString(1, "foo");
		stmt.execute();
		ResultSet rs = stmt.getResultSet();
		rs.next();
		Assert.assertEquals("foo", rs.getString(3));
		rs.close();
		conn.commit();
		stmt.setString(1, "bar");
		stmt.execute();
		rs = stmt.getResultSet();
		rs.next();
		Assert.assertEquals("bar", rs.getString(3));
		stmt.close();
		conn.commit();
		conn.close();
	}

	@Test
	public void testCPAAS166() throws Exception {
		Connection conn = createConnection();

		String t1 = "T1(cntr_id long, minCharge double, whifsc_id string, scenario_id string)@python = {*\n";
		t1 = t1 + "\tyield(1, float(1052860764),'6984659189','13')\n";
		t1 = t1 + "\tyield(2, float(3605622112),'6910707508','29')\n";
		t1 = t1 + "\tyield(3, float(3738204326),'6937707871','9')\n";
		t1 = t1 + "\tyield(4, float(3834933825),'6984373124','42')\n";
		t1 = t1 + "\tyield(5, float(3892383175),'6951466386','19')\n";
		t1 = t1 + "\tyield(6, float(4163312844),'6934099889','40')\n";
		t1 = t1 + "\tyield(7, float(4415235109),'6938154491','12')\n";
		t1 = t1 + "\tyield(8, float(5518430208),'6946384258','1')\n";
		t1 = t1 + "\tyield(9, float(5541651213),'6959060778','1')\n";
		t1 = t1 + "\tyield(10,float(5613442243),'6964282295','24')\n";
		t1 = t1 + "\tyield(11,float(6482158223),'6958294735','40')\n";
		t1 = t1 + "\tyield(12,float(8466189493),'6928674143','22')\n";
		t1 = t1 + " *}\n";

		String t2 = "T2(cntr_id long, minCharge double, whifsc_id string, scenario_id string)@python = {*\n";
		t2 = t2 + "\tyield(1,float(3834933825),'1088.73','26')\n";
		t2 = t2 + "\tyield(2,float(3738204326),'968.09','25')\n";
		t2 = t2 + "\tyield(3,float(5518430208),'896.7199999999999','26')\n";
		t2 = t2 + "\tyield(4,float(1052860764),'761.0','26')\n";
		t2 = t2 + "\tyield(5,float(6482158223),'1010.51','25')\n";
		t2 = t2 + "\tyield(6,float(3605622112),'926.37','25')\n";
		t2 = t2 + "\tyield(7,float(3892383175),'968.6300000000001','25')\n";
		t2 = t2 + "\tyield(8,float(5613442243),'1422.7600000000002','26')\n";
		t2 = t2 + "\tyield(9,float(5541651213),'1055.5900000000001','26')\n";
		t2 = t2 + "\tyield(10,float(4415235109),'826.86','25')\n";
		t2 = t2 + "\tyield(11,float(8466189493),'1032.0','26')\n";
		t2 = t2 + "\tyield(12,float(4163312844),'942.83','26')\n";
		t2 = t2 + " *}\n";

		String query = t1 + t2 + " SELECT * FROM T1 JOIN T2 ON T1.cntr_id=T2.cntr_id";
		PreparedStatement stmt = conn.prepareStatement(query);
		stmt.execute();
		ResultSet rs = stmt.getResultSet();
		int count = 0;
		while (rs.next()) {
			count++;
		}
		rs.close();
		stmt.close();
		conn.commit();
		conn.close();

		Assert.assertTrue(count == 12);
	}

	@Test
	public void testCPAAS169() throws Exception {
		Connection conn = createConnection();

		String t1 = "T1(cntr_id long, minCharge double, whifsc_id string, scenario_id string)@python = {*\n";
		t1 = t1 + "\tyield(1, float(1052860764),'6984659189','13')\n";
		t1 = t1 + "\tyield(2, float(3605622112),'6910707508','29')\n";
		t1 = t1 + "\tyield(3, float(3738204326),'6937707871','9')\n";
		t1 = t1 + "\tyield(4, float(3834933825),'6984373124','42')\n";
		t1 = t1 + "\tyield(5, float(3892383175),'6951466386','19')\n";
		t1 = t1 + "\tyield(6, float(4163312844),'6934099889','40')\n";
		t1 = t1 + "\tyield(7, float(4415235109),'6938154491','12')\n";
		t1 = t1 + "\tyield(8, float(5518430208),'6946384258','1')\n";
		t1 = t1 + "\tyield(9, float(5541651213),'6959060778','1')\n";
		t1 = t1 + "\tyield(10,float(5613442243),'6964282295','24')\n";
		t1 = t1 + "\tyield(11,float(6482158223),'6958294735','40')\n";
		t1 = t1 + "\tyield(12,float(8466189493),'6928674143','22')\n";
		t1 = t1 + " *}\n";

		String query = t1 + " SELECT * FROM T1";
		PreparedStatement stmt = conn.prepareStatement(query);
		for (int i = 0; i < 3; i++) {
			stmt.execute();
			ResultSet rs = stmt.getResultSet();
			int count = 0;
			while (rs.next()) {
				count++;
			}
			Assert.assertTrue(count == 12);
			rs.close();
		}
		stmt.close();
		conn.commit();
		conn.close();
	}

}
