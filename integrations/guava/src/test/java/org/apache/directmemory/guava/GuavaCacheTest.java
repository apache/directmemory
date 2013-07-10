package org.apache.directmemory.guava;

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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.directmemory.DirectMemory;
import org.apache.directmemory.cache.CacheService;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class GuavaCacheTest
{

    private int maxSize = 100;

    private int offHeapSize = 20;

    private Cache<Integer, Integer> primaryCache;

    private CacheService<Integer, Integer> cacheService;

    private OffHeapCache<Integer, Integer> cache;

    @Test
    public void testEviction()
    {
        createScenario();

        //No of entries in off heap cache should be equal to the extra entries
        //over and above the maxSize specified for Cache as those would be evicted
        assertEquals( offHeapSize, cacheService.entries() );

        for ( int i = 0; i < maxSize + offHeapSize; i++ )
        {
            Integer ignored = cache.getIfPresent( i );
        }

        assertEquals( offHeapSize, cache.offHeapStats().loadCount() );
    }

    @Test
    public void testWithLoader()
        throws ExecutionException
    {
        createScenario();

        Callable<Integer> testCallable = new Callable<Integer>()
        {
            @Override
            public Integer call()
                throws Exception
            {
                return -1;
            }
        };

        Integer result = cache.get( 1000, testCallable );
        assertEquals( new Integer( -1 ), result );
    }

    @Test
    public void testInvalidate()
    {
        createScenario();

        //Find a key which is actually part of L2
        Integer key = Iterables.get( cacheService.getMap().keySet(), 0 );

        cache.invalidate( key );

        assertNull( cacheService.retrieve( key ) );
    }

    @Test
    public void testInvalidateAllWithKeys()
    {
        createScenario();
        Integer keyInL1 = Iterables.get( cache.asMap().keySet(), 0 );
        Integer keyInL2 = Iterables.get( cacheService.getMap().keySet(), 0 );

        Iterable<Integer> keys = Lists.newArrayList( keyInL1, keyInL2 );
        cache.invalidateAll( keys );

        assertNull( cacheService.retrieve( keyInL2 ) );
        assertNull( cache.getIfPresent( keyInL1 ) );
    }

    @Test
    public void testInvalidateAll()
    {
        createScenario();
        cache.invalidateAll();
        assertEquals( 0, cacheService.entries() );
    }

    @Test
    public void getAllPresent()
    {
        createScenario();
        Integer keyInL1 = Iterables.get( cache.asMap().keySet(), 0 );
        Integer keyInL2 = Iterables.get( cacheService.getMap().keySet(), 0 );

        Iterable<Integer> keys = Lists.newArrayList( keyInL1, keyInL2, 1000 );
        Map<Integer, Integer> result = cache.getAllPresent( keys );

        assertEquals( 2, result.size() );
        assertEquals( result.get( keyInL1 ), keyInL1 );
        assertEquals( result.get( keyInL2 ), keyInL2 );

        Iterable<Integer> keys2 = Lists.newArrayList( keyInL1 );
        Map<Integer, Integer> result2 = cache.getAllPresent( keys2 );

        assertEquals( 1, result2.size() );
        assertEquals( result2.get( keyInL1 ), keyInL1 );
    }

    private void createScenario()
    {
        //1. First create a bridge listener
        ForwardingListener<Integer, Integer> listener = ForwardingListener.newInstance();

        //2. Second create the Guava cache with the bridge listener
        primaryCache = CacheBuilder.newBuilder().maximumSize( maxSize ).removalListener( listener ).build();

        //3. Create the CacheService
        cacheService = new DirectMemory<Integer, Integer>().setNumberOfBuffers( 10 ).setSize( 1000 ).setInitialCapacity(
            10000 ).setConcurrencyLevel( 4 ).newCacheService();

        //4. Create the L2 cache with bridge listener and Guava cache
        cache = new OffHeapCache<Integer, Integer>( cacheService, primaryCache, listener );

        //Add number of entries so as to cause overflow and thus trigger eviction
        for ( int i = 0; i < maxSize + offHeapSize; i++ )
        {
            cache.put( i, i );
        }
    }

}
