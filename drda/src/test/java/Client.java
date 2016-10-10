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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;


public class Client {

	public static void main(String[] args) throws Exception{
	    String EXTERNAL_DRIVER = "org.apache.derby.jdbc.ClientDriver";
	    try {
	        Class.forName(EXTERNAL_DRIVER);
	    } catch (ClassNotFoundException e) {
	        throw new SQLException("Could not find class " + EXTERNAL_DRIVER);
	    }

	    Connection conn = DriverManager.getConnection(
	    		"jdbc:derby://localhost:1527/seconddb;create=true;");

	    String t1 = "T1( x int, y string )@python = {* yield (1, 'abc') *} \n";
	    String t2 = "T2( x int, y string ) = ( SELECT T1.x, T1.y FROM T1 ) \n";
	    String query = t1 + t2 +"SELECT T2.x, T2.y FROM T2 WHERE T2.x = 1";
	    PreparedStatement stmt = null; 
	    ResultSet rs = null;
	    try{
	        stmt = conn.prepareStatement(query);
	        stmt.execute();
	        rs = stmt.getResultSet();
			
	        while (rs.next()) {
	            System.out.println("row " + rs.getRow() + ": " + rs.getInt(1)
	                + ", " + rs.getString(2));
	        }
	    }finally{
	        if(rs != null) rs.close();
	        if(stmt != null) stmt.close();
	        if(conn != null) conn.close();
	    }

	}

}
