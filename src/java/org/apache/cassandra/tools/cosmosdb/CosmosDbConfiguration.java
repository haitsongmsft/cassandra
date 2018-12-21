package org.apache.cassandra.tools.cosmosdb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * CosmosDbConfiguration utility to read the configurations from properties file
 */
public class CosmosDbConfiguration 
{
	
    private static final Logger LOGGER = LoggerFactory.getLogger(CosmosDbConfiguration.class);
    private static String PROPERTY_FILE = "cosmosdbconfig.properties";
    private static Properties prop = null;

    private void loadProperties() throws IOException 
    {
        InputStream input = getClass().getClassLoader().getResourceAsStream(PROPERTY_FILE);
        if (input == null) 
        {
            LOGGER.error("Sorry, unable to find {}", PROPERTY_FILE);
            return;
        }
        
        prop = new Properties();
        prop.load(input);
    }

    public String getProperty(String propertyName) throws IOException 
    {
        if (prop == null) 
        {
            loadProperties();
        }
        
        return prop.getProperty(propertyName);
    }
}
