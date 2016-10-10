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

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;
import java.util.Set;

import org.apache.derby.impl.drda.DRDAConnThread;
import org.apache.derby.impl.drda.NetworkServerControlImpl;
import org.apache.derby.impl.drda.Session;

import eu.coherentpaas.cqe.datastore.DataStore;
import eu.coherentpaas.cqe.server.CloudMdSQLManager;
import eu.coherentpaas.transactionmanager.minicluster.TMMiniCluster;

public final class CloudMdSQLServer extends NetworkServerControl {

	private static CloudMdSQLServer instance = null;
	private URL leanxcaleProperties = null;

	public CloudMdSQLServer() throws Exception {

		leanxcaleProperties = Thread.currentThread().getContextClassLoader()
				.getResource("leanxcale-site.xml");
		if (leanxcaleProperties == null) {
			TMMiniCluster.startMiniCluster(true, 1, false);
		}
		/*else{
			TMMiniCluster.startMiniCluster(true, 0, false);
		}*/
		URL url = Thread.currentThread().getContextClassLoader()
				.getResource("wrappers");
		if (url == null) {
			throw new RuntimeException(
					"The wrappers directory is not available to load the datastores configurations");
		}
		String config = url.getFile();

		File configFile = new File(config);
		File[] files = configFile.listFiles();

		for (int i = 0; i < files.length; i++) {
			if (files[i].getName().endsWith(".properties")) {
				Properties prop = new Properties();
				InputStream in = new FileInputStream(files[i]);
				Class<?> wrapperClass = null;
				try {
					prop.load(in);
					if (prop.containsKey("wrapper.class")) {
						try {
							wrapperClass = Class.forName(prop
									.getProperty("wrapper.class"));

						} catch (ClassNotFoundException e) {
							System.out.println("ERROR: The class ["
									+ prop.getProperty("wrapper.class")
									+ "] is not found!");
						}
						Object object = wrapperClass.newInstance();
						if (object instanceof DataStore) {
							DataStore ds = (DataStore) object;
							String name = prop.getProperty("wrapper.name");
							CloudMdSQLManager.getInstance().addDataStore(name,
									ds, prop);

						} else {
							System.out.println("ERROR: The class ["
									+ prop.getProperty("wrapper.class")
									+ "] is not found!");
						}

					}

				} finally {
					in.close();
				}
			}
		}

		url = Thread.currentThread().getContextClassLoader()
				.getResource("homes.properties");
		if (url != null) {
			config = url.getFile();
			configFile = new File(config);
			Properties prop = new Properties();
			InputStream in = new FileInputStream(configFile);
			try {
				prop.load(in);
			} finally {
				in.close();
			}
			CloudMdSQLManager manager = CloudMdSQLManager.getInstance();
			Set<Object> keys = prop.keySet();
			if (keys != null) {
				for (Object key : keys) {
					String value = prop.get(key).toString();
					String name = key.toString();
					if (name.startsWith("*")) {
						name = name.substring(1);
						manager.setLocalHome(name);
					}
					int index = value.indexOf(':');
					int port = 1527;
					String host = value;
					if (index != -1) {
						try {
							port = Integer.parseInt(value.substring(index + 1));
						} catch (NumberFormatException e) {
							throw new RuntimeException(
									"Invalid connection URL for "
											+ key.toString()
											+ ".The format must be host:port");
						}
						host = value.substring(0, index);
					}
					manager.addRemoteHome(name, host, port);
				}
			}
			if (manager.getLocalHome() == null) {
				throw new RuntimeException(
						"There is no home marked with * to set the local one");
			}
		}

		setServer(new NetworkServerControlImpl() {
			public DRDAConnThread createConnectionThread(Session session) {
				return new MdSQLDRDAConnThread(session, this, getTimeSlice(),
						getLogConnections());
			}
			/*
			@Override
			 public String getCloudScapeDriver(){
				 return CQEInternalDriver.class.getName();
			 }*/
		});

	}

	Thread shutdown = new Thread() {
		@Override
		public void run() {

			try {
				instance.halt();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	};

	public void halt() throws Exception {
		CloudMdSQLManager.getInstance().shutdown();
		Thread thread = new Thread() {
			@Override
			public void run() {

				try {
					System.out
							.println("Stopping the TMMiniCluster...Please, wait");
					if (leanxcaleProperties == null) {
						TMMiniCluster.stopMiniCluster();
					}

				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		};

		thread.start();

	}

	/**
	 * main routine for NetworkServerControl
	 *
	 * @param args
	 *            array of arguments indicating command to be executed. See
	 *            class comments for more information
	 */
	public static void main(String args[]) {

		try {
			if (instance == null) {
				instance = new CloudMdSQLServer();
				Runtime.getRuntime().addShutdownHook(instance.shutdown);

			}
			if ("shutdown".equals(args[0])
					&& instance.leanxcaleProperties == null) {
				instance.shutdown();

			}
			
			run(instance, args);

		} catch (Exception e) {

			e.printStackTrace();
			System.exit(1);
		}
	}

	public static CloudMdSQLServer getInstance() {
		return instance;
	}
	

}
