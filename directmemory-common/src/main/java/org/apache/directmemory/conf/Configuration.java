package org.apache.directmemory.conf;

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
        return configurationService.getNumberOfBuffers();
    }

    public static int getInitialCapacity()
    {
        return configurationService.getInitialCapacity();
    }

    public static int getRamMegaBytes()
    {
        return configurationService.getRamMegaBytes();
    }

    public static long getDisposalTime()
    {
        return configurationService.getDisposalTime();
    }

    public static int getConcurrencyLevel()
    {
        return configurationService.getConcurrencyLevel();
    }
    
    private static ConfigurationService configurationService;
    
    static
    {
        if (configurationService == null) {
            // if not otherwise specified with another mechanism it uses the default implementation
            logger.info( "using default configuration implementation" );
            configurationService = new SimplePropertiesConfiguration();
        }
    }

    // Prevent instance escape
    private Configuration()
    {
    }

    public static void configureFromYaml()
    {
        logger.info( "using yaml configuration implementation" );
        configurationService = YamlConfiguration.load();
    }

}
