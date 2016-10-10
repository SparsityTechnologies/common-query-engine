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
package edu.upc.dama.test.memory.leaks;



import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Enumeration;

import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.coherentpaas.transactionmanager.client.LTMServerProxy;
import eu.coherentpaas.transactionmanager.exception.DataStoreException;
import eu.coherentpaas.transactionmanager.exception.TransactionManagerException;
/**
 *
 * @author idezol
 */
@Ignore
public class CQETest2OpenAndClose {
     private static final Logger Log = LoggerFactory.getLogger(CQETest2OpenAndClose.class);
     
     private LTMServerProxy proxy;
     
     private Integer threads = 10;
    
    @Test
    public void firstExample() throws UnknownHostException, TransactionManagerException, DataStoreException, SQLException {
        Log.info("Starting minicluster, in case it is not yet started");
       // TMMiniCluster.startMiniCluster(true, 1);
        
        long time0 = 0;
		long time1 = 0;
		long time2 = 0;
		long time3 = 0;
		long oidDoc = 0;
		
		int i = 0;
        
		while(true){
			try {
				
				time0 = 0;
				time1 = 0;
				time2 = 0;
				time3 = 0;
				oidDoc = 0;
				
				Enumeration<Driver> enume = DriverManager.getDrivers();
				while (enume.hasMoreElements()) {
					Driver driver = enume.nextElement();
					if (driver.toString().contains("Sparksee")) {
						DriverManager.deregisterDriver(driver);
					}
				}

				Connection conn = DriverManager.getConnection("jdbc:derby://" + "localhost"
						+ ":1527/seconddb;create=true;;queryLang=cloudmdsql;");

				conn.setAutoCommit(false);
				
				String query = "createnode(oid long WITHPARAMS docid string)@python = {*\n"
						+ "\tyield(1L)\n"
						+ "*}\n"
						+ "select oid from createnode('1')";

				PreparedStatement stmt = conn.prepareStatement(query);

				
				time0 = System.currentTimeMillis();
				//stmt.setString(1, idDoc);
				stmt.execute();
				time1 = System.currentTimeMillis();
	
				ResultSet rs = stmt.getResultSet();
	
				if (rs.next()) {
					oidDoc = rs.getLong(1);
				}
				rs.close();
	
				time2 = System.currentTimeMillis();
				
				
				conn.commit();
				
				stmt.close();
				
				
				conn.close();
				
				conn = null;
				rs = null;
				stmt = null;
				
				time3 = System.currentTimeMillis();
				
				System.out.println((oidDoc+i)+"\t"+(time1-time0)+" execute Document\t"+(time2-time1)+" getResultSet Document\t"+ (time3-time2)+" commit Document");
				i++;
			} catch (Exception e) {
				time1 = System.currentTimeMillis();
				if(time0!=0){
					System.out.println((time1-time0)+" execute Document\t"+(time2-time1)+" getResultSet Document\t"+ (time3-time2)+" commit Document");
				}
				throw new RuntimeException();
			}
		}
        
//        proxy=  new LTMServerProxy();
//        
//        TxnCtx ctx;
//        ctx = proxy.startTransaction();
//		ctx.commit();
        
        

        
       // TMMiniCluster.stopMiniCluster();
    }
    
   
    
    
}
