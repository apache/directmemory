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

package org.apache.directmemory.tests.osgi.cache;

import org.apache.directmemory.DirectMemory;
import org.apache.directmemory.cache.CacheService;
import org.apache.directmemory.measures.Ram;
import org.apache.directmemory.memory.AllocationPolicy;
import org.apache.directmemory.memory.MemoryManagerService;
import org.apache.directmemory.memory.MemoryManagerServiceWithAllocationPolicyImpl;
import org.apache.directmemory.memory.RoundRobinAllocationPolicy;
import org.apache.directmemory.serialization.SerializerFactory;
import org.apache.directmemory.serialization.StandardSerializer;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;

import java.util.Properties;

public class CacheServiceExportingActivator
    implements BundleActivator
{

    CacheService<String, SimpleObject> cacheService;

    @Override
    public void start( BundleContext context )
        throws Exception
    {
        AllocationPolicy allocationPolicy = new RoundRobinAllocationPolicy();
        MemoryManagerService<SimpleObject> memoryManager =
            new MemoryManagerServiceWithAllocationPolicyImpl<SimpleObject>( allocationPolicy, true );
        this.cacheService =
            new DirectMemory<String, SimpleObject>().setNumberOfBuffers( 1 ).setSize( Ram.Mb( 1 ) ).setMemoryManager(
                memoryManager ).setSerializer(
                SerializerFactory.createNewSerializer( StandardSerializer.class ) ).newCacheService();
        cacheService.put( "1", new SimpleObject( "1,", "Activator Object" ) );
        context.registerService( CacheService.class.getCanonicalName(), cacheService, new Properties() );

        /*CacheService<Integer, byte[]> cache = new DirectMemory<Integer, byte[]>()
        .setMemoryManager( memoryManager )
        .setNumberOfBuffers( 1 )
        .setSize(  )
        .newCacheService();*/
    }

    @Override
    public void stop( BundleContext context )
        throws Exception
    {
    }
}
