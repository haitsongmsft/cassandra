package org.apache.cassandra.tools.cosmosdb;

import com.datastax.driver.core.*;
import javax.net.ssl.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.*;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import org.apache.cassandra.config.EncryptionOptions;

/**
 * Cassandra utility class to handle the Cassandra Sessions
 */
public class CosmosIngester implements Runnable
{
	
    private Cluster cluster;
    private CosmosDbConfiguration config = new CosmosDbConfiguration();    
    private BlockingQueue<String> queue = new ArrayBlockingQueue<String>(6000);

    /**
     * This method creates a Cassandra Session based on the the end-point details given in config.properties.
     * This method validates the SSL certificate based on ssl_keystore_file_path & ssl_keystore_password properties.
     * If ssl_keystore_file_path & ssl_keystore_password are not given then it uses 'cacerts' from JDK.
     * @return Session Cassandra Session
     * @throws IOException 
     */
    public Session getSession() throws IOException 
    {
        String host = config.getProperty("cassandra_host");
        int port = Integer.parseInt(config.getProperty("cassandra_port"));
        String user = config.getProperty("cassandra_username");
        String pass = config.getProperty("cassandra_password");
        String sslpath = config.getProperty("ssl_keystore_file_path");
        String sslpass = config.getProperty("ssl_keystore_password");
        return getCosmosDbSession(host, port, user, pass, sslpath,sslpass );
    }

    public Cluster getCluster()
    {
        return cluster;
    }

    /**
     * Closes the cluster and Cassandra session
     */
    public void close() 
    {
        cluster.close();
    }
    
    /**
     * Loads Cassandra SSL key file.
     * @throws Exception
     */
    private File loadSSLKeyFile( String ssl_keystore_file_path) throws Exception {

        // If ssl_keystore_file_path, build the path using JAVA_HOME directory.
        if (ssl_keystore_file_path == null || ssl_keystore_file_path.isEmpty()) {
            String javaHomeDirectory = System.getenv("JAVA_HOME");
            if (javaHomeDirectory == null || javaHomeDirectory.isEmpty()) {
                throw new Exception("JAVA_HOME not set");
            }
            ssl_keystore_file_path = new StringBuilder(javaHomeDirectory).append("/jre/lib/security/cacerts").toString();
        }

        File sslKeyStoreFile = new File(ssl_keystore_file_path);

        if (!sslKeyStoreFile.exists() || !sslKeyStoreFile.canRead()) {
            throw new Exception(String.format("Unable to access the SSL Key Store file from %s", ssl_keystore_file_path));
        }
        
        return sslKeyStoreFile;
    }       
        
    /**
     * This method creates a Cassandra Session based on the the end-point details given in config.properties.
     * This method validates the SSL certificate based on ssl_keystore_file_path & ssl_keystore_password properties.
     * If ssl_keystore_file_path & ssl_keystore_password are not given then it uses 'cacerts' from JDK.
     * @return Session Cassandra Session
     */
    private Session getCosmosDbSession(
            String host,
            int port,
            String username,
            String password,
            String ssl_keystore_file_path,
            String ssl_keystore_password) {

        try {
            //Load cassandra endpoint details from config.properties
            File sslKeyStoreFile = loadSSLKeyFile(ssl_keystore_file_path);
            
            String sslKeyStorePassword = "changeit";
            sslKeyStorePassword = (ssl_keystore_password != null && !ssl_keystore_password.isEmpty()) ?
                    ssl_keystore_password : sslKeyStorePassword;
            
            final KeyStore keyStore = KeyStore.getInstance("JKS");
            try (final InputStream is = new FileInputStream(sslKeyStoreFile)) {
                keyStore.load(is, sslKeyStorePassword.toCharArray());
            }

            final KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory
                    .getDefaultAlgorithm());
            kmf.init(keyStore, sslKeyStorePassword.toCharArray());
            final TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory
                    .getDefaultAlgorithm());
            tmf.init(keyStore);

            // Creates a socket factory for HttpsURLConnection using JKS contents.
            final SSLContext sc = SSLContext.getInstance("TLSv1.2");
            sc.init(kmf.getKeyManagers(), tmf.getTrustManagers(), new java.security.SecureRandom());

            EncryptionOptions encryptionOptions = new EncryptionOptions.ClientEncryptionOptions();
            SSLOptions sslOptions = JdkSSLOptions.builder()
                    .withSSLContext(sc)
                    .withCipherSuites(encryptionOptions.cipher_suites).build();
                        
            cluster = Cluster.builder()
                    .addContactPoint(host)
                    .withPort(port)
                    .withCredentials(username, password)
                    .withSSL(sslOptions)
                    .build();

            return cluster.connect();
            
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }
    
    private String GetBatchQueryContent(BatchStatement batch) 
    {
    	if(batch==null) return null;
    	StringBuilder sb = new StringBuilder();
    	for(Statement st : batch.getStatements()) 
    	{
    		sb.append(st.toString());
    		sb.append("\n");
    	}    	
    	return sb.toString();
    }
        
    public void run()
    {
    	try 
    	{
    		int maxRetries = 1;
    	    int startInterval = 5000;
    		int numRetries = 0;
    		int sleepInterval = startInterval;
    		BatchStatement batch = null;
	    	while(true) 
	    	{
	    		try 
	    		{
	    			
	    			if(batch==null) 
	    			{
	    				batch = createBatch();
	    			}
	    			
		        	if(batch.size()==0)
		        	{
		        		return;
		        	}
	            	else 
	            	{
				    	Session session = this.getSession();
				    	for(Statement st : batch.getStatements()) {
				    		System.out.println(">>["+numRetries+"]: "+st.toString());
				    	    session.execute(st);
				    		System.out.println("<<["+numRetries+"]: "+st.toString());
				    	}
	            		/*
				    	ResultSetFuture res = session.executeAsync(batch);
			    		System.out.println(">>["+numRetries+"]: "+ this.GetBatchQueryContent(batch));
				    	ResultSet resx =res.get();
			    		System.out.println("<<["+numRetries+"]: "+ this.GetBatchQueryContent(batch));
				    	*/
				    	batch =  null;
	            	}
	    		}
	    		catch(Exception ex) 
	    		{
	    			// retry batch;
	    			if(++numRetries>maxRetries) 
	    			{
	    				System.err.println("BatchError: " + ex.getMessage());
	    				System.err.println("FailedBatch: " + this.GetBatchQueryContent(batch));
	    				numRetries = 0;
	    				sleepInterval = startInterval;
				    	batch =  null;
	    			}
	    			else 
	    			{
	    				Thread.sleep(sleepInterval);
	    				sleepInterval *=2;
	    			}
	    		}
	    	}
    	}
    	catch(InterruptedException e) 
    	{    		
    	}
    }
                
    private synchronized BatchStatement createBatch() 
    		throws InterruptedException
    {
    	int nMaxBatchSize = 100;
    	BatchStatement batch = new BatchStatement(); 
    	char batchType = 0;
    	
    	for(int i=0; i<nMaxBatchSize; i++)
    	{
    		if(queue.isEmpty() && batch.size() >0 )
    		{
    			return batch;
    		}
        	String qry = queue.take();
    		if(qry.length()==0 || (qry.charAt(0) != batchType && batchType>0))
    		{
    			queue.put(qry); // put back, so as to all other thread will quit
    			return batch;
    		}
    		else 
    		{
    			// we want to build batch in the same type, all inserts go together
    			batchType = qry.charAt(0); 
    		    batch.add(new SimpleStatement(qry));
    		}
    	}
    	return batch;
    }
        
    public void addStatement(String statement)
    {
    	try 
    	{
    		queue.put(statement);    		
    	}
    	catch(InterruptedException ex) 
    	{
    		// eat and retry;
    		System.err.println("Failed to queue statement: " + statement );
    	}
    }
}
