package org.apache.directmemory.ehcache;

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

import net.sf.ehcache.CacheException;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Ehcache;
import net.sf.ehcache.Element;

import org.junit.Assert;
import org.junit.Test;

public class EHCacheTest
{

    @Test
    public void testPutRetreive()
    {
        CacheManager cacheManager = CacheManager.getInstance();
        Ehcache ehcache = cacheManager.getEhcache( "testCache" );

        ehcache.put( new Element( "testKey", "testValue" ) );
        stats( ehcache );
        Assert.assertEquals( "testValue", ehcache.get( "testKey" ).getObjectValue() );
    }

    @Test
    public void testSizing()
    {
        CacheManager cacheManager = CacheManager.getInstance();
        Ehcache ehcache = cacheManager.getEhcache( "testCache" );
        for ( int i = 0; i < 30000; i++ )
        {
            if ( ( i % 1000 ) == 0 )
            {
                System.out.println( "heatbeat " + i );
                stats( ehcache );
            }
            ehcache.put( new Element( i, new byte[1024] ) );
        }
        stats( ehcache );
        Assert.assertTrue( true );
    }

    @Test
    public void testOffHeapExceedMemory()
        throws IOException
    {
        CacheManager cacheManager = CacheManager.getInstance();
        Ehcache ehcache = cacheManager.getEhcache( "testCache" );
        Element element = null;
        try
        {
            for ( int i = 0; i < 3000000; i++ )
            {
                if ( ( i % 1000 ) == 0 )
                {
                    System.out.println( "heatbeat " + i );
                    stats( ehcache );
                }
                element = new Element( i, new byte[1024] );
                ehcache.put( element );
            }
            Assert.fail( "CacheException expected for DirectMemory OffHeap Memory Exceeded" );
        }
        catch ( CacheException e )
        {
            stats( ehcache );
            Assert.assertTrue( "CacheException expected for DirectMemory OffHeap Memory Exceeded", true );
        }

    }

    private void stats( Ehcache ehcache )
    {
        System.out.println( "OnHeapSize=" + ehcache.calculateInMemorySize() + ", OnHeapElements="
            + ehcache.getMemoryStoreSize() );
        System.out.println( "OffHeapSize=" + ehcache.calculateOffHeapSize() + ", OffHeapElements="
            + ehcache.getOffHeapStoreSize() );
        System.out.println( "DiskStoreSize=" + ehcache.calculateOnDiskSize() + ", DiskStoreElements="
            + ehcache.getDiskStoreSize() );
    }

}
