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
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cassandra utility class to handle the Cassandra Sessions
 */
public class CosmosIngester implements Runnable
{
	
    private static final Logger logger = LoggerFactory.getLogger(CosmosIngester.class);
    private Cluster cluster;
    private CosmosDbConfiguration config = new CosmosDbConfiguration();    
    private BlockingQueue<RetryQuery> queue = new ArrayBlockingQueue<RetryQuery>(6000);
    private BlockingQueue<RetryQuery> retryQueue = new ArrayBlockingQueue<RetryQuery>(600);
    private BlockingQueue<Session> connectSession = new ArrayBlockingQueue<Session>(10);

    /**
     * This method creates a Cassandra Session based on the the end-point details given in config.properties.
     * This method validates the SSL certificate based on ssl_keystore_file_path & ssl_keystore_password properties.
     * If ssl_keystore_file_path & ssl_keystore_password are not given then it uses 'cacerts' from JDK.
     * @return Session Cassandra Session
     * @throws IOException 
     * @throws InterruptedException 
     */
    public Session getSession() throws IOException, InterruptedException 
    {
    	if(connectSession.isEmpty()) 
    	{
            String host = config.getProperty("cassandra_host");
            int port = Integer.parseInt(config.getProperty("cassandra_port"));
            String user = config.getProperty("cassandra_username");
            String pass = config.getProperty("cassandra_password");
            String sslpath = config.getProperty("ssl_keystore_file_path");
            String sslpass = config.getProperty("ssl_keystore_password");
            return getCosmosDbSession(host, port, user, pass, sslpath,sslpass);    		
    	}
    	else 
    	{
    		return connectSession.take();
    	}
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
            
    public void run()
    {
		int batchEnds = 0;
    	while(true) 
    	{
    		try 
    		{
    			if(batchEnds <0 && this.retryQueue.isEmpty()) 
    			{
    				return;
    			}
    			
    			if(this.retryQueue.isEmpty())
    			{
    				batchEnds = createAndExecuteBatch();
    			}    			
    			else
    			{
    				this.retryStatements();
    			}	        	
    		}
    		catch(Exception ex) 
    		{
    			logger.error(ex.getMessage());
    		}
    	}	
    }
    
    private void retryStatements()
    {
    	try 
    	{
		    int startInterval = 5000;
			if(!this.retryQueue.isEmpty())
			{
				RetryQuery qry = this.retryQueue.take();
				Thread.sleep(qry.Count*startInterval);
				this.executeStatement(qry);
			}
    	}
    	catch(InterruptedException ex) 
    	{
    	}
    }

    public void executeStatement(String qry) throws InterruptedException
    {
    	this.executeStatement(new RetryQuery(qry, 0));
    }
    
    private void executeStatement(RetryQuery qry) throws InterruptedException
    {
    	Session session = null;
    	try 
    	{
    		qry.Count++;
    		session = this.getSession();
    		session.execute(new SimpleStatement(qry.ToCqlQuery()));
    		System.out.println("RETRY:" + qry.ToCqlQuery() + " Succeeded" + qry.Count);
    	}
    	catch(Exception ex)
    	{
    		if(session!=null) 
    		{
     		    this.connectSession.put(session);
    		}
    		if(qry.Count <5) 
    		{
        		this.retryQueue.add(qry);    			
    		}
    		else 
    		{
    			System.err.println(qry.ToCqlQuery() + " failed: " + ex.getMessage());
    		}
    	}
    }
    
	private String GetFieldNameValueList(String baseInsertQuery, ObjectNode rowNode, ArrayList<Object> values)
    {
    	StringBuilder names = new StringBuilder();
    	StringBuilder bindPlaces = new StringBuilder();
    	names.append(baseInsertQuery);
    	names.append("(");
    	int startLen = names.length();
    	bindPlaces.append(" values (");
    	rowNode.getFields().forEachRemaining(fv->{
    		String fieldName = fv.getKey();
    		String sep = names.length()>startLen ? ", ": "";
    		JsonNode nodeValue = fv.getValue();
    		if(nodeValue.isNull()) 
    		{
    		    values.add(null);
    		}
    		else if(nodeValue.isFloatingPointNumber())
    		{
    		    values.add(nodeValue.asDouble());    			
    		}
    		else if(nodeValue.isBigInteger()) 
    		{
    			values.add(nodeValue.asLong());
    		}
    		else if(nodeValue.isInt()) 
    		{
    			values.add(nodeValue.asInt());
    		}
    		else if(nodeValue.isBoolean()) 
    		{
    			values.add(nodeValue.asBoolean());
    		}
    		else if(nodeValue.isNumber()) 
    		{
    		    values.add(nodeValue.getNumberValue());
    		}
    		else 
    		{
        		String textNodeValue = nodeValue.asText();
        		values.add(textNodeValue);
    		}    		
    		names.append(sep + fieldName);
        	bindPlaces.append(sep + "?");
    	});
    	names.append(") ");
    	bindPlaces.append(")    ");
    	names.append(bindPlaces);
    	return names.toString(); 
    }
    
    private void executeInsertStatements( java.util.ArrayList<RetryQuery> queries) throws InterruptedException
    {
		java.util.ArrayList<Object> objValues = new java.util.ArrayList<Object>();
		StringBuilder sb = new StringBuilder();
		String newLine = System.getProperty("line.separator");
		Session session = null;
    	try 
    	{    		
    		session = this.getSession();
    		sb.append("BEGIN UNLOGGED BATCH ");
    		sb.append(newLine);
    		queries.forEach(x->{
    		   String sqry = this.GetFieldNameValueList(x.Qry, x.rowNode, objValues);
    		   sb.append(sqry);
               sb.append(newLine);
    		});
    		sb.append("APPLY BATCH;");
            sb.append(newLine);
        	PreparedStatement st = session.prepare(sb.toString());
    		BoundStatement bd = st.bind(objValues.toArray());
    	    ResultSet response = session.execute(bd);
            if (response != null && response.getAllExecutionInfo() != null)
            {
            }
            logger.info("Inserted: " + queries.size());
    	}
    	catch(Exception ex)
    	{
    		if(session!=null) 
    		{
    			this.connectSession.put(session);
    		}
    		System.err.println(ex.getMessage());
    		this.retryQueue.addAll(queries);
    	}
    }
    
    private synchronized int createAndExecuteBatch() throws InterruptedException, IOException
    {
    	
		java.util.ArrayList<RetryQuery> qryStatements = new java.util.ArrayList<RetryQuery>();
		
    	int nMaxBatchSize = 100;
    	int isEndingBatch = 0;
    	String nonInsertQry = null;
    	
    	for(int i=0; i<nMaxBatchSize; i++)
    	{
    		
    		RetryQuery query = queue.take();    		
    		if(query.Qry.length()==0)
    		{
    			queue.put(query); // put back, so as to all other thread will quit
    			isEndingBatch = -1;
    			break;
    		}
    		
    		String qry = query.Qry;
    		
			// we want to build batch in the same type, all inserts go together
			char batchType = qry.charAt(0); 
			if(batchType=='i') 
			{
				qryStatements.add(query);
			}
			else 
			{
				nonInsertQry = qry;
				break;
			}
    	}
    	
    	// build an insert batch:    	
    	if(qryStatements.size() > 0) 
    	{
    		this.executeInsertStatements(qryStatements);
    	}
    	
    	// execute other query;
    	if(nonInsertQry!=null) 
    	{
    		executeStatement(nonInsertQry);
    	} 
    	
    	return isEndingBatch;
    }
        
    public void addStatement(String statement)
    {
    	try 
    	{
            logger.info("adding statement: /" + statement + "/");
    		queue.put(new RetryQuery(statement,0));    		
    	}
    	catch(InterruptedException ex) 
    	{
    		// eat and retry;
    		logger.error("Failed to queue statement: " + statement );
    	}
    }
    
    // add a record for bulk inserts.
    public void addInsertRow(String insertTableStatement, ObjectNode rowNode)
    {
    	this.queue.add(new RetryQuery(insertTableStatement, rowNode));
    }
    
    private class RetryQuery
    {
    	String Qry;
    	ObjectNode rowNode;
    	int Count;
    	RetryQuery(String q, int c, ObjectNode rowNode) { this.Qry = q; this.Count =c; this.rowNode = rowNode; }
    	RetryQuery(String q, ObjectNode rowNode) { this(q,0,rowNode); }
    	RetryQuery(String q, int c) { this(q,c,null); }
    	RetryQuery(String q) { this(q, 0); }
    	
    	public String ToCqlQuery() 
    	{
    		if(this.rowNode !=null && Qry.startsWith("i")) 
    		{
                String insertRowAsJson = this.Qry + " JSON '{" + CosmosDbTransformer.assembleFields(rowNode, false) +"}'";
    			return insertRowAsJson;
    		}
    		return Qry;
    	}
    	
    }    
}
