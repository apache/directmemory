package org.apache.directmemory.conf;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

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

/**
 * Provides a global single point of access to all singleton properties comprehensible to DirectMemory.
 */
public final class Configuration
{
    private static final Logger logger = LoggerFactory.getLogger( Configuration.class );

    public static int getNumberOfBuffers()
    {
        return yamlShadow.numberOfBuffers;
    }

    public static int getInitialCapacity()
    {
        return yamlShadow.initialCapacity;
    }

    public static int getRamMegaBytes()
    {
        return yamlShadow.ramMegaBytes;
    }

    public static long getDisposalTime()
    {
        return yamlShadow.disposalTime;
    }

    public static int getConcurrencyLevel()
    {
        return yamlShadow.concurrencyLevel;
    }

    private static void wireConfiguration()
    {
        boolean success = false;
        InputStream inputStream = null;
        String yamlLocation = null;
        try
        {
            yamlLocation =
                new File( System.getProperty( "user.dir" ) + "/../conf/directmemory.yaml" ).getCanonicalPath();
            inputStream = new FileInputStream( new File( yamlLocation ) );
            Yaml yaml = new Yaml( new Constructor( YamlShadow.class ) );
            yamlShadow = (YamlShadow) yaml.load( inputStream );
            success = true;
        }
        catch ( Exception exception )
        {
            logger.error( "Problem trying to load DirectMemory configuration, now exiting.", exception );
        }
        finally
        {
            if ( inputStream != null )
            {
                try
                {
                    inputStream.close();
                }
                catch ( IOException ioException )
                { // no-op
                }
            }
            if ( success )
            {
                logger.info( "Loading DirectMemory configuration from " + yamlLocation );
            }
        }
    }

    public static class YamlShadow
    {
        public int numberOfBuffers = 1;

        public int initialCapacity = 100000;

        public int ramMegaBytes = 1;

        public int concurrencyLevel = 4;

        public long disposalTime = 10L;

        public YamlShadow()
        {
        }
    }

    private static YamlShadow yamlShadow;
    static
    {
        wireConfiguration();
    }

    // Prevent instance escape
    private Configuration()
    {
    }

}
