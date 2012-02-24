package org.apache.directmemory;

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

import static org.apache.directmemory.DirectMemory.createNewInstance;

import org.junit.Test;

public final class BootstrapTestCase
{

    @Test( expected = IllegalArgumentException.class )
    public void mustNotCreateAnything()
    {
        createNewInstance( null );
    }

    @Test( expected = DirectMemoryConfigurationException.class )
    public void wrongMemorySize()
    {
        createNewInstance( new CacheConfiguration<String, String>()
        {

            @Override
            public void configure( CacheConfigurator<String, String> cacheConfigurator )
            {
                cacheConfigurator.allocateMemoryOfSize( -10 ).Gb();
            }

        } );
    }

    @Test( expected = DirectMemoryConfigurationException.class )
    public void wrongMemorySizeViaAbstractConfiguration()
    {
        createNewInstance( new AbstractCacheConfiguration<String, String>()
        {

            @Override
            public void configure()
            {
                allocateMemoryOfSize( -10 ).Gb();
            }

        } );
    }

    @Test( expected = DirectMemoryConfigurationException.class )
    public void wrongScheduleDisposal()
    {
        createNewInstance( new CacheConfiguration<String, String>()
        {

            @Override
            public void configure( CacheConfigurator<String, String> cacheConfigurator )
            {
                cacheConfigurator.scheduleDisposal().every( 0 ).days();
            }

        } );
    }

    @Test( expected = DirectMemoryConfigurationException.class )
    public void wrongScheduleDisposalViaAbstractConfiguration()
    {
        createNewInstance( new AbstractCacheConfiguration<String, String>()
        {

            @Override
            public void configure()
            {
                scheduleDisposal().every( 0 ).days();
            }

        } );
    }

}
