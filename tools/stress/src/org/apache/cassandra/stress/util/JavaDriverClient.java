/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.stress.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.security.KeyStore;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.net.ssl.SSLContext;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import com.azure.cosmosdb.cassandra.util.Configurations;
import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.core.policies.WhiteListPolicy;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Slf4JLoggerFactory;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.security.SSLFactory;
import org.apache.cassandra.stress.settings.StressSettings;

public class JavaDriverClient
{

    static
    {
        InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory());
    }

    public final String host;
    public final int port;
    public final String username;
    public final String password;
    public final AuthProvider authProvider;
    public final int maxPendingPerConnection;
    public final int connectionsPerHost;

    private final ProtocolVersion protocolVersion;
    private final EncryptionOptions.ClientEncryptionOptions encryptionOptions;
    private Cluster cluster;
    private Session session;
    private final LoadBalancingPolicy loadBalancingPolicy;

    private static final ConcurrentMap<String, PreparedStatement> stmts = new ConcurrentHashMap<>();

    public JavaDriverClient(StressSettings settings, String host, int port)
    {
        this(settings, host, port, new EncryptionOptions.ClientEncryptionOptions());
    }

    public JavaDriverClient(StressSettings settings, String host, int port, EncryptionOptions.ClientEncryptionOptions encryptionOptions)
    {
        this.protocolVersion = settings.mode.protocolVersion;
        this.host = host;
        this.port = port;
        this.username = settings.mode.username;
        this.password = settings.mode.password;
        this.authProvider = settings.mode.authProvider;
        this.encryptionOptions = encryptionOptions;
        this.loadBalancingPolicy = loadBalancingPolicy(settings);
        this.connectionsPerHost = settings.mode.connectionsPerHost == null ? 8 : settings.mode.connectionsPerHost;

        int maxThreadCount = 0;
        if (settings.rate.auto)
            maxThreadCount = settings.rate.maxThreads;
        else
            maxThreadCount = settings.rate.threadCount;

        //Always allow enough pending requests so every thread can have a request pending
        //See https://issues.apache.org/jira/browse/CASSANDRA-7217
        int requestsPerConnection = (maxThreadCount / connectionsPerHost) + connectionsPerHost;

        maxPendingPerConnection = settings.mode.maxPendingPerConnection == null ? Math.max(128, requestsPerConnection ) : settings.mode.maxPendingPerConnection;
    }

    private LoadBalancingPolicy loadBalancingPolicy(StressSettings settings)
    {
        DCAwareRoundRobinPolicy.Builder policyBuilder = DCAwareRoundRobinPolicy.builder();
        if (settings.node.datacenter != null)
            policyBuilder.withLocalDc(settings.node.datacenter);

        LoadBalancingPolicy ret = null;
        if (settings.node.datacenter != null)
            ret = policyBuilder.build();

        if (settings.node.isWhiteList)
            ret = new WhiteListPolicy(ret == null ? policyBuilder.build() : ret, settings.node.resolveAll(settings.port.nativePort));

        return new TokenAwarePolicy(ret == null ? policyBuilder.build() : ret);
    }

    public PreparedStatement prepare(String query)
    {
        PreparedStatement stmt = stmts.get(query);
        if (stmt != null)
            return stmt;
        synchronized (stmts)
        {
            stmt = stmts.get(query);
            if (stmt != null)
                return stmt;
            stmt = getSession().prepare(query);
            stmts.put(query, stmt);
        }
        return stmt;
    }

    public void connect(ProtocolOptions.Compression compression) throws Exception
    {
	   	if(host.contains("cosmosdb.azure.com")) {
	  		this.session = this.getCosmosDbSession( host, port, username, password, null, null);
    		return;
	   	}
	   	
        PoolingOptions poolingOpts = new PoolingOptions()
                                     .setConnectionsPerHost(HostDistance.LOCAL, connectionsPerHost, connectionsPerHost)
                                     .setMaxRequestsPerConnection(HostDistance.LOCAL, maxPendingPerConnection)
                                     .setNewConnectionThreshold(HostDistance.LOCAL, 100);

        Cluster.Builder clusterBuilder = Cluster.builder()
                                                .addContactPoint(host)
                                                .withPort(port)
                                                .withPoolingOptions(poolingOpts)
                                                .withoutJMXReporting()
                                                .withProtocolVersion(protocolVersion)
                                                .withoutMetrics(); // The driver uses metrics 3 with conflict with our version
        if (loadBalancingPolicy != null)
            clusterBuilder.withLoadBalancingPolicy(loadBalancingPolicy);
        clusterBuilder.withCompression(compression);
        if (encryptionOptions.enabled)
        {
            SSLContext sslContext;
            sslContext = SSLFactory.createSSLContext(encryptionOptions, true);
            SSLOptions sslOptions = JdkSSLOptions.builder()
                                                 .withSSLContext(sslContext)
                                                 .withCipherSuites(encryptionOptions.cipher_suites).build();
            clusterBuilder.withSSL(sslOptions);
        }

        if (authProvider != null)
        {
            clusterBuilder.withAuthProvider(authProvider);
        }
        else if (username != null)
        {
            clusterBuilder.withCredentials(username, password);
        }

        cluster = clusterBuilder.build();
        Metadata metadata = cluster.getMetadata();
        System.out.printf(
                "Connected to cluster: %s, max pending requests per connection %d, max connections per host %d%n",
                metadata.getClusterName(),
                maxPendingPerConnection,
                connectionsPerHost);
        for (Host host : metadata.getAllHosts())
        {
            System.out.printf("Datatacenter: %s; Host: %s; Rack: %s%n",
                    host.getDatacenter(), host.getAddress(), host.getRack());
        }

        session = cluster.connect();
    }

    public Cluster getCluster()
    {
        return cluster;
    }

    public Session getSession()
    {
        return session;
    }

    public ResultSet execute(String query, org.apache.cassandra.db.ConsistencyLevel consistency)
    {
        SimpleStatement stmt = new SimpleStatement(query);
        stmt.setConsistencyLevel(from(consistency));
        return getSession().execute(stmt);
    }

    public ResultSet executePrepared(PreparedStatement stmt, List<Object> queryParams, org.apache.cassandra.db.ConsistencyLevel consistency)
    {
        stmt.setConsistencyLevel(from(consistency));
        BoundStatement bstmt = stmt.bind((Object[]) queryParams.toArray(new Object[queryParams.size()]));
        return getSession().execute(bstmt);
    }

    /**
     * Get ConsistencyLevel from a C* ConsistencyLevel. This exists in the Java Driver ConsistencyLevel,
     * but it is not public.
     *
     * @param cl
     * @return
     */
    public static ConsistencyLevel from(org.apache.cassandra.db.ConsistencyLevel cl)
    {
        switch (cl)
        {
            case ANY:
                return com.datastax.driver.core.ConsistencyLevel.ANY;
            case ONE:
                return com.datastax.driver.core.ConsistencyLevel.ONE;
            case TWO:
                return com.datastax.driver.core.ConsistencyLevel.TWO;
            case THREE:
                return com.datastax.driver.core.ConsistencyLevel.THREE;
            case QUORUM:
                return com.datastax.driver.core.ConsistencyLevel.QUORUM;
            case ALL:
                return com.datastax.driver.core.ConsistencyLevel.ALL;
            case LOCAL_QUORUM:
                return com.datastax.driver.core.ConsistencyLevel.LOCAL_QUORUM;
            case EACH_QUORUM:
                return com.datastax.driver.core.ConsistencyLevel.EACH_QUORUM;
            case LOCAL_ONE:
                return com.datastax.driver.core.ConsistencyLevel.LOCAL_ONE;
        }
        throw new AssertionError();
    }

    public void disconnect()
    {
        cluster.close();
    }
    
    /**
     * This method creates a Cassandra Session based on the the end-point details given in config.properties.
     * This method validates the SSL certificate based on ssl_keystore_file_path & ssl_keystore_password properties.
     * If ssl_keystore_file_path & ssl_keystore_password are not given then it uses 'cacerts' from JDK.
     * @return Session Cassandra Session
     */
    public Session getCosmosDbSession(
            String host,
            int port,
            String username,
            String password,
            String ssl_keystore_file_path,
            String ssl_keystore_password) {

        try {
            //Load cassandra endpoint details from config.properties
            File sslKeyStoreFile = loadCassandraConnectionDetails(ssl_keystore_file_path);
            
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

            SSLOptions sslOptions = JdkSSLOptions.builder()
                    .withSSLContext(sc)
                    .withCipherSuites(encryptionOptions.cipher_suites).build();
            
            //JdkSSLOptions sslOptions = RemoteEndpointAwareJdkSSLOptions.builder()
            //        .withSSLContext(sc)
            //        .build();
            
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
    
    /**
     * This method creates a Cassandra Session based on the the end-point details given in config.properties.
     * This method validates the SSL certificate based on ssl_keystore_file_path & ssl_keystore_password properties.
     * If ssl_keystore_file_path & ssl_keystore_password are not given then it uses 'cacerts' from JDK.
     * @return Session Cassandra Session
     */
    private Session ___getCosmosDbSession(
            String host,
            int port,
            String username,
            String password,
            String ssl_keystore_file_path,
            String ssl_keystore_password) {

        try {
            //Load cassandra endpoint details from config.properties
            File sslKeyStoreFile = loadCassandraConnectionDetails(ssl_keystore_file_path);
            
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

            JdkSSLOptions sslOptions = RemoteEndpointAwareJdkSSLOptions.builder()
                    .withSSLContext(sc)
                    .build();
            
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
    

    /**
     * Closes the cluster and Cassandra session
     */
    public void close() {
        cluster.close();
    }

    /**
     * Loads Cassandra end-point details from config.properties.
     * @throws Exception
     */
    private File loadCassandraConnectionDetails(
    		String ssl_keystore_file_path) throws Exception {

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
}
