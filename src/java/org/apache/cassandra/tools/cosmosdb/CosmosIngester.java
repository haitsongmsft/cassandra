package org.apache.cassandra.tools.cosmosdb;

import com.datastax.driver.core.*;

import javax.net.ssl.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.security.*;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.cassandra.config.EncryptionOptions;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cassandra utility class to handle the Cassandra Sessions
 */
public class CosmosIngester implements Runnable
{
    private static final Logger logger = LoggerFactory.getLogger(CosmosIngester.class);
	private static final int RETRYINTERVAL = 2000; // miliseconds;
	private static final int MAXRETRIES = 3;
	private static final int NSESSIONS = 4;
    private static BlockingQueue<Session> connectSession = null;
    // private static BlockingQueue<CosmosIngester> ingesters = null;
    private static Cluster cluster =  null;
    private static CosmosDbConfiguration config = new CosmosDbConfiguration();    
    
    // working queue for requests.
    private BlockingQueue<RetryQuery> queue = new ArrayBlockingQueue<RetryQuery>(6000);
    
    // internal working queue for retry requests, whenever failure happens, this queue
    // is retried before queue is processed.
    private BlockingQueue<RetryQuery> retryQueue = new ArrayBlockingQueue<RetryQuery>(6000);
    
    private boolean isEnding = false;
    private static boolean isKilled = false; // terminating by kill 

    /**
     * This method creates a Cassandra Session based on the the end-point details given in config.properties.
     * This method validates the SSL certificate based on ssl_keystore_file_path & ssl_keystore_password properties.
     * If ssl_keystore_file_path & ssl_keystore_password are not given then it uses 'cacerts' from JDK.
     * @return Session Cassandra Session
     * @throws IOException 
     * @throws InterruptedException 
     */
    public static Session getSession() throws IOException, InterruptedException 
    {
    	if(connectSession == null) 
    	{
            String host = config.getProperty("cassandra_host");
            int port = Integer.parseInt(config.getProperty("cassandra_port"));
            String user = config.getProperty("cassandra_username");
            String pass = config.getProperty("cassandra_password");
            String sslpath = config.getProperty("ssl_keystore_file_path");
            String sslpass = config.getProperty("ssl_keystore_password");
            connectSession = new ArrayBlockingQueue<Session>(NSESSIONS);
    		for(int i=0; i<NSESSIONS; i++) 
    		{
    			Session s = getCosmosDbSession(host, port, user, pass, sslpath,sslpass); 
    			connectSession.add(s);
    		}
    	}
		return connectSession.take();
    }
    
    /*
    public static CosmosIngester GetInstance() throws InterruptedException
    {
    	CosmosIngester inj = null;
    	// we allow max number of partitions being worked on, each of partition uses its own fresh
    	// CosmosIngester, within which will be single threaded.
    	if(ingesters == null) 
    	{
    		ingesters = new ArrayBlockingQueue<CosmosIngester>(NTHREADS);
    		for(int i=0; i<NTHREADS; i++) 
    		{
    			inj = new CosmosIngester();
    			ingesters.add(inj);
    		}
    	}
    	inj = ingesters.take();
    	inj.clear();
    	return inj;
    }
    
    public static boolean SaveInstance(CosmosIngester ing)
    {
    	if(ing==null) return false;
    	return ingesters.add(ing);
    }
    */
    
    private static boolean SaveSession(Session s)
    {
    	if(s==null) return false;
    	return connectSession.add(s);    	
    }
        
    /**
     * Loads Cassandra SSL key file.
     * @throws Exception
     */
    private static File loadSSLKeyFile( String ssl_keystore_file_path) 
    		throws IOException 
    {

        // If ssl_keystore_file_path, build the path using JAVA_HOME directory.
        if (ssl_keystore_file_path == null || ssl_keystore_file_path.isEmpty()) {
            String javaHomeDirectory = System.getenv("JAVA_HOME");
            if (javaHomeDirectory == null || javaHomeDirectory.isEmpty()) {
                throw new IOException("JAVA_HOME not set");
            }
            ssl_keystore_file_path = new StringBuilder(javaHomeDirectory).append("/jre/lib/security/cacerts").toString();
        }

        File sslKeyStoreFile = new File(ssl_keystore_file_path);

        if (!sslKeyStoreFile.exists() || !sslKeyStoreFile.canRead()) {
            throw new IOException(String.format("Unable to access the SSL Key Store file from %s", ssl_keystore_file_path));
        }
        
        return sslKeyStoreFile;
    }   
            
    /**
     * This method creates a Cassandra Session based on the the end-point details given in config.properties.
     * This method validates the SSL certificate based on ssl_keystore_file_path & ssl_keystore_password properties.
     * If ssl_keystore_file_path & ssl_keystore_password are not given then it uses 'cacerts' from JDK.
     * @return Session Cassandra Session
     */
    private static Session getCosmosDbSession(
            String host,
            int port,
            String username,
            String password,
            String ssl_keystore_file_path,
            String ssl_keystore_password) throws IOException
    {
        try 
        {        	
        	if (cluster == null) 
        	{        	
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
        	}

            return cluster.connect();
            
        } catch (Exception ex) {
            throw new IOException(ex.getMessage());
        }
    }
    
    public static void Close() 
    {
    	while(!connectSession.isEmpty())
    	{
    		try 
    		{
    			connectSession.take().close();
    		}
    		catch(InterruptedException e)
    		{
    			// eat exception;
    		}
    	}
    	cluster.close();
    }
    
    private void clear()
    {
    	isEnding = false; // before fresh run, clean up
    	preparedInsertQry = null; // fresh run, clean up
    	retryQueue.clear();
    	queue.clear();
    }
            
    public void run()
    {
    	while( !isKilled )
    	{
    		try 
    		{
    			if( this.isEnding && this.retryQueue.isEmpty() && this.queue.isEmpty())
    			{
    				return;
    			}     			
    			if(!this.retryQueue.isEmpty())
    			{
    				this.retryStatements();
    			}
    			else if(!this.queue.isEmpty()) // only if retry is empty, we will proceed with new work.
    			{
    				createAndExecuteBatch();
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
			if(!this.retryQueue.isEmpty())
			{
				RetryQuery qry = this.retryQueue.take();
	    		qry.SleepOnRetry();
				this.executeStatement(qry);
			}
    	}
    	catch(InterruptedException ex) 
    	{
    		// eat exception;
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
    		session = getSession();
    		session.execute(new SimpleStatement(qry.ToCqlQuery()));
    		logger.debug("EXECUTESUCESS@{}: {}", qry.Count, qry.ToCqlQuery());
    	}
    	catch(Exception ex)
    	{
    		if(qry.Count<= MAXRETRIES)
    		{
	    		this.retryQueue.add(qry);    			
	    		logger.error("RETRYFAIL@{}: {} {}", qry.Count, qry.ToCqlQuery(), ex.getMessage());
    		}
    		else
    		{
        		logger.warn("RETRYWARN@{}: {} {}",  qry.Count, qry.ToCqlQuery(), ex.getMessage());
    		}
    	}
    	finally
    	{
    		SaveSession(session);
    	}
    }
            
	/* insert by bulk using unlogged batch with ensuring the partition keys are the same across records */
    private void executePreparedUnloggedBatch( ArrayList<RetryQuery> queries) 
    		throws InterruptedException
    {
		ArrayList<Object> objValues = new ArrayList<Object>();
		StringBuilder sb = new StringBuilder();
		String newLine = System.getProperty("line.separator");
 	    String insertQry = null; 
		Session session = null;
    	try 
    	{    		
    		session = getSession();
    		sb.append("BEGIN UNLOGGED BATCH ");
    		sb.append(newLine);
    		for(RetryQuery x: queries)
    		{
    		   insertQry = insertQry == null? GetInsertPrepareStatement(x.Qry, x.rowNode): insertQry;
    		   sb.append(insertQry);
               sb.append(newLine);
     		   objValues.addAll(GetStatementBindValues(x.rowNode));
    		};
    		sb.append("APPLY BATCH;");
            sb.append(newLine);
        	PreparedStatement st = session.prepare(sb.toString());
    		BoundStatement bd = st.bind(objValues.toArray());
    	    ResultSet resultSet = session.execute(bd);
    	    ExecutionInfo exc = resultSet.getExecutionInfo();
    	    java.util.Map<String, ByteBuffer> incomingPayLoad = exc.getIncomingPayload();
    	    ByteBuffer valueInBytes = incomingPayLoad.get("RequestCharge");
    	    double requestUnits = valueInBytes.asDoubleBuffer().get();
            logger.info("Inserted: " + queries.size() + " CosmosDb Request Units: " + requestUnits);
    	}
    	catch(Exception ex)
    	{
    		System.err.println(ex.getMessage());
    		this.retryQueue.addAll(queries);
    	}
    	finally
    	{
    		SaveSession(session);
    	}
    }

    private void executeJsonUnloggedBatch( ArrayList<RetryQuery> queries) 
    		throws InterruptedException
    {
		StringBuilder sb = new StringBuilder();
		String newLine = System.getProperty("line.separator");
		Session session = null;
    	try 
    	{    		
    		session = getSession();
    		// we are safe to use unlogged batch as we are in same partition keys
    		// the method below directly uses JSON as input, which requires server
    		// to parse JSON. vs the above uses one layer of column name broke down 
    		// and it does NOT seem to have major difference.
    		sb.append("BEGIN UNLOGGED BATCH "); 
    		sb.append(newLine);
    		for(RetryQuery x: queries)
    		{
    			sb.append(x.ToCqlQuery());
        		sb.append(newLine);
    		};
    		sb.append("APPLY BATCH;");
            sb.append(newLine);
    	    ResultSet resultSet = session.execute(sb.toString());
    	    ExecutionInfo exc = resultSet.getExecutionInfo();
    	    java.util.Map<String, ByteBuffer> incomingPayLoad = exc.getIncomingPayload();
    	    ByteBuffer valueInBytes = incomingPayLoad.get("RequestCharge");
    	    double requestUnits = valueInBytes.asDoubleBuffer().get();
            logger.info("Inserted: " + queries.size() + " CosmosDb Request Units: " + requestUnits);
    	}
    	catch(Exception ex)
    	{
    		System.err.println(ex.getMessage());
    		this.retryQueue.addAll(queries);
    	}
    	finally
    	{
    		SaveSession(session);
    	}
    }
        
    private PreparedStatement preparedInsertQry = null;
    
	/*************************************************************************************************************** 
	 * insert by prepared statements, it is said to be more efficient than previous ones due to time saved in parsing
	 * however, due to the ingester is going to live within a partition, so the unlogged batch could work faster.
	 * https://medium.com/@foundev/cassandra-batch-loading-without-the-batch-the-nuanced-edition-dd78d61e9885
	 ***************************************************************************************************************
    */
    private void executePreparedInsertStatements( java.util.ArrayList<RetryQuery> queries) throws InterruptedException
    {
		Session session = null;
    	try 
    	{    		
    		session = getSession();
    		ArrayList<ResultSetFuture> resList = new ArrayList<ResultSetFuture>();
    		for(RetryQuery x : queries)
    		{
	            ArrayList<Object> objValues = GetStatementBindValues(x.rowNode);
	            if(null == preparedInsertQry) 
	            {
		            String spqry = GetInsertPrepareStatement(x.Qry, x.rowNode);
	            	preparedInsertQry = session.prepare(spqry); 
	            }
	    		BoundStatement bd = preparedInsertQry.bind(objValues.toArray());
	    	    ResultSetFuture future = session.executeAsync(bd);
	    	    resList.add(future);
    		}
    		resList.forEach(x->{
    			ResultSet resultSet = x.getUninterruptibly();
        	    ExecutionInfo exc = resultSet.getExecutionInfo();
        	    java.util.Map<String, ByteBuffer> incomingPayLoad = exc.getIncomingPayload();
        	    ByteBuffer valueInBytes = incomingPayLoad.get("RequestCharge");
        	    double requestUnits = valueInBytes.asDoubleBuffer().get();
                logger.info(" CosmosDb Request Units: " + requestUnits);
    		});    		
    		logger.info( "Inserted: " + queries.size());
        }
    	catch(Exception ex)
    	{
    		logger.error(ex.getMessage());
    		this.retryQueue.addAll(queries);
    	}
    	finally
    	{
    		SaveSession(session);
    	}
    }
    
    private void createAndExecuteBatch() throws InterruptedException, IOException
    {    	
    	java.util.ArrayList<RetryQuery> qryStatements = new java.util.ArrayList<RetryQuery>();		
    	int nMaxBatchSize = 1000;
    	String nonInsertQry = null;
    	
    	for(int i=0; i<nMaxBatchSize && !queue.isEmpty(); i++)
    	{
    		
    		RetryQuery query = queue.take();    		
    		String qry = query.Qry;    		
			// we want to build batch in the same type, all inserts go together
			char batchType = qry.charAt(0); 
			if(batchType=='i') 
			{
				qryStatements.add(query);
			}
			else 
			{
				// we build inserts into a batch, if anything other than insert, we execute whatever we have so far.
				nonInsertQry = qry; 
				break;
			}
    	}
    	
    	// build an insert batch:    	
    	if(qryStatements.size() > 0) 
    	{
    		/* these 2 unlogged batch showed same RU consumption*/
    		// this.executePreparedUnloggedBatch(qryStatements);
    		this.executeJsonUnloggedBatch(qryStatements);
    		// 
    		// this.executePreparedInsertStatements(qryStatements);
    	}
    	
    	// execute other query;
    	if(nonInsertQry!=null) 
    	{
    		executeStatement(nonInsertQry);
    	}    	
    }
    
    public void end()
    {
    	this.isEnding = true;
    }

    public static void kill()
    {
    	isKilled = true;
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
    	throws InterruptedException
    {
    	this.queue.put(new RetryQuery(insertTableStatement, rowNode));
    }

    public static String GetInsertPrepareStatement(String baseInsertQuery, ObjectNode rowNode)
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
            names.append(sep + fieldName);
            bindPlaces.append(sep + "?");
        });
        names.append(") ");
        bindPlaces.append(")    ");
        names.append(bindPlaces);
        return names.toString(); 
    }
    
    public static ArrayList<Object> GetStatementBindValues(ObjectNode rowNode)
    {        
        ArrayList<Object> values = new ArrayList<Object>();
        rowNode.getFields().forEachRemaining(fv->{
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
        });
        return values;
    }
    
    private class RetryQuery
    {
    	String Qry;
    	ObjectNode rowNode;
    	int Count;
    	    	
    	RetryQuery(String q, int c, ObjectNode rowNode) 
    	{ 
    		this.Qry = q; 
    		this.Count =c; 
    		this.rowNode = rowNode;
    	}
    	
    	void SleepOnRetry()
    	{
    		try 
    		{
    			Thread.sleep( this.Count*RETRYINTERVAL+1000 );			
			}
    		catch(Exception ex)
			{			
			}
    	}
    	
    	RetryQuery(String q, ObjectNode rowNode) { this(q,0,rowNode); }
    	RetryQuery(String q, int c) { this(q,c,null); }
    	
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
