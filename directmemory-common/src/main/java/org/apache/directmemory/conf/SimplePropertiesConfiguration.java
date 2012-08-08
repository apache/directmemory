package org.apache.directmemory.conf;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimplePropertiesConfiguration extends DefaultConfiguration implements ConfigurationService
{
    private static final Logger logger = LoggerFactory.getLogger( SimplePropertiesConfiguration.class );

    public SimplePropertiesConfiguration()
    {
        Properties properties = new Properties();
        try
        {
            properties.load( this.getClass().getClassLoader().getResourceAsStream( "directmemory.properties" ) );
            this.setNumberOfBuffers(new Integer(properties.getProperty( "numberOfBuffers" )));
            this.setInitialCapacity(new Integer(properties.getProperty( "initialCapacity" )));
            this.setRamMegaBytes(new Integer(properties.getProperty( "ramMegaBytes" )));
            this.setConcurrencyLevel(new Integer(properties.getProperty( "concurrencyLevel" )));
            this.setDisposalTime(new Long(properties.getProperty( "disposalTime" )));
        }
        catch ( Exception e )
        {
            // nothing - keep the defaults and warn the world about it
            logger.warn( "no properties file found or invalid file - using defaults" );
        }
    }
 
}
