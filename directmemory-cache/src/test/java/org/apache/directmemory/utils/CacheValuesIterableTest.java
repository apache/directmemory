package org.apache.directmemory.utils;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.io.IOException;
import java.util.Iterator;

import org.apache.directmemory.DirectMemory;
import org.apache.directmemory.cache.CacheService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Testcase for {@link CacheValuesIterable}
 */
public class CacheValuesIterableTest
{
    private CacheService<String, Long> cache;

    @Before
    public void setUp() {
        cache = new DirectMemory<String, Long>().setNumberOfBuffers( 10 ).setSize( 1000 ).setInitialCapacity( 10000 ).setConcurrencyLevel( 4 ).newCacheService(); 
    }
    
    @After
    public void cleanUp() throws IOException {
        cache.clear();
        cache.close();
    }

    @Test
    public void simpleStrictCacheValuesIteratorTest()
        throws Exception
    {
        assertNull( cache.retrieve( "a" ) );
        assertNotNull( cache.put( "a", 1L ) );
        assertNotNull( cache.put( "b", 2L ) );
        assertNotNull( cache.put( "c", 3L ) );
        assertNotNull( cache.retrieve( "a" ) );
        assertNotNull( cache.retrieve( "b" ) );
        assertNotNull( cache.retrieve( "c" ) );
        assertEquals( 1L, cache.retrieve( "a" ).longValue() );

        CacheValuesIterable<String, Long> cacheValuesIterable = new CacheValuesIterable<String, Long>( cache );
        for ( Long longVal : cacheValuesIterable )
        {
            assertNotNull( longVal );
            assertTrue( longVal > 0 );
        }
    }

    @Test
    public void simpleNonStrictCacheValuesIteratorTest()
        throws Exception
    {
        assertNull( cache.retrieve( "a" ) );
        assertNotNull( cache.put( "a", 1L ) );
        assertNotNull( cache.put( "b", 2L ) );
        assertNotNull( cache.put( "c", 3L ) );
        assertNotNull( cache.retrieve( "a" ) );
        assertNotNull( cache.retrieve( "b" ) );
        assertNotNull( cache.retrieve( "c" ) );
        assertEquals( 1L, cache.retrieve( "a" ).longValue() );

        CacheValuesIterable<String, Long> cacheValuesIterable = new CacheValuesIterable<String, Long>( cache, false );
        int count = 0;
        for ( Long longVal : cacheValuesIterable )
        {
            count++;
            assertNotNull( longVal );
            assertTrue( longVal > 0 );
        }
        assertEquals( 3, count );
    }

    @Test
    public void nonStrictCacheValuesIteratorShouldSkipExpiredItemsTest() throws Exception
    {
        assertNotNull( cache.put( "a", 1L ) );
        assertNotNull( cache.put( "b", 2L, 1 ) );
        assertNotNull( cache.put( "c", 3L ) );

        Thread.sleep( 10 );
        CacheValuesIterable<String, Long> cacheValuesIterable = new CacheValuesIterable<String, Long>( cache, false );
        int count = 0;
        for ( Long longVal : cacheValuesIterable )
        {
            count++;
            assertNotNull( longVal );
            assertTrue( longVal > 0 );
        }
        assertEquals( 2, count );
    }
    
    @Test
    public void nonStrictCacheValuesIteratorRemoveTest() throws Exception {
        assertNotNull( cache.put( "a", 1L ) );
        assertNotNull( cache.put( "b", 2L ) );
        assertNotNull( cache.put( "c", 3L ) );

        Iterator<Long> iterator = new CacheValuesIterable<String, Long>( cache, false ).iterator();
        while ( iterator.hasNext() )
        {
            long longVal = iterator.next();
            if (longVal == 2) {
                iterator.remove();
            }
        }
        int count = 0;
        iterator = new CacheValuesIterable<String, Long>( cache, false ).iterator();
        while (iterator.hasNext()) {
            iterator.next();
            count++;
        }
        assertEquals( 2, count);

    }
}
