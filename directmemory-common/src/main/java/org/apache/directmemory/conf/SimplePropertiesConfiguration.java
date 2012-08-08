package org.apache.directmemory.conf;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class SimplePropertiesConfiguration
    extends DefaultConfiguration
    implements ConfigurationService
{
    private static final Logger logger = LoggerFactory.getLogger( SimplePropertiesConfiguration.class );

    public SimplePropertiesConfiguration()
    {
        Properties properties = new Properties();
        try
        {
            properties.load( this.getClass().getClassLoader().getResourceAsStream( "directmemory.properties" ) );
            this.setNumberOfBuffers( new Integer( properties.getProperty( "numberOfBuffers" ) ) );
            this.setInitialCapacity( new Integer( properties.getProperty( "initialCapacity" ) ) );
            this.setRamMegaBytes( new Integer( properties.getProperty( "ramMegaBytes" ) ) );
            this.setConcurrencyLevel( new Integer( properties.getProperty( "concurrencyLevel" ) ) );
            this.setDisposalTime( new Long( properties.getProperty( "disposalTime" ) ) );
        }
        catch ( Exception e )
        {
            // nothing - keep the defaults and warn the world about it
            logger.warn( "no properties file found or invalid file - using defaults" );
        }
    }

}
