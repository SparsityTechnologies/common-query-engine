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

import eu.coherentpaas.cqe.server.TableStoreInMemoryImpl;

public class TestMultiThreading {

	
	public void testCPAAS162() throws Exception {
		TableStoreInMemoryImpl tt = new TableStoreInMemoryImpl();
		for (int i = 0; i < 10000; i++) {
			Object[][] row = new Object[0][0];
			tt.writeNextRow(row);
		}
		
		Thread[] threads = new Thread[20];
		for(int i = 0; i < 20; i++){
			threads[i] = new Thread(new Consumer(tt));
			threads[i].start();
		}
		while(true){}

	}
	
	public class Consumer implements Runnable{

		private TableStoreInMemoryImpl tt;
		
		public Consumer(TableStoreInMemoryImpl tt){
			this.tt = tt;
		}
		
		@Override
		public void run() {
			while(true){
				try {
					tt.readNextRow();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		
	}
}
