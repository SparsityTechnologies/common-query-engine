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
import java.sql.SQLException;

import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.coherentpaas.transactionmanager.client.LTMServerProxy;
import eu.coherentpaas.transactionmanager.client.TxnCtx;
import eu.coherentpaas.transactionmanager.exception.DataStoreException;
import eu.coherentpaas.transactionmanager.exception.TransactionManagerException;
import eu.coherentpaas.transactionmanager.minicluster.TMMiniCluster;
/**
 *
 * @author idezol
 */
@Ignore
public class CQETest {
     private static final Logger Log = LoggerFactory.getLogger(CQETest.class);
     
     private LTMServerProxy proxy;
     
     private Integer threads = 10;
    
    @Test
    public void firstExample() throws UnknownHostException, TransactionManagerException, DataStoreException, SQLException {
        Log.info("Starting minicluster, in case it is not yet started");
        TMMiniCluster.startMiniCluster(true, 1);
        
        proxy=  new LTMServerProxy();
        
        for(int i = 0;i<threads;i++){
        	SimpleThread st = new SimpleThread();
        	st.start();
        }
        
        while(threads>0){
        	try {
        	    Thread.sleep(5000);
        	} catch(InterruptedException ex) {
        	    Thread.currentThread().interrupt();
        	}
        }
        
        TMMiniCluster.stopMiniCluster();
    }
    
    class SimpleThread extends Thread {
    	public boolean run = true;
        public SimpleThread() {
        	super();
        }
        public void run() {
        	while(run){
        		TxnCtx ctx;
				try {
					ctx = proxy.startTransaction();
					ctx.commit();
				} catch (TransactionManagerException e) {
					synchronized(threads){
						threads --;
						run = false;
					}
				}
				System.out.println("DONE! " + getName());
        	}
        }
    }
    
    
}
