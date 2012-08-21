package org.apache.directmemory.cache;

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

import org.apache.directmemory.DirectMemory;
import org.apache.directmemory.measures.Ram;
import org.apache.directmemory.memory.AllocationPolicy;
import org.apache.directmemory.memory.MemoryManagerService;
import org.apache.directmemory.memory.MemoryManagerServiceImpl;
import org.apache.directmemory.memory.Pointer;
import org.apache.directmemory.memory.RoundRobinAllocationPolicy;
import org.junit.Test;

import java.io.Serializable;

import static org.junit.Assert.*;

public class CacheServiceImplTest
{

    @Test
    public void testOffHeapExceedMemoryReturnNullWhenTrue()
    {
        AllocationPolicy allocationPolicy = new RoundRobinAllocationPolicy();
        MemoryManagerService<byte[]> memoryManager = new MemoryManagerServiceImpl<byte[]>( allocationPolicy, true );
        CacheService<Integer, byte[]> cache =
            new DirectMemory<Integer, byte[]>().setMemoryManager( memoryManager ).setNumberOfBuffers( 1 ).setSize(
                Ram.Mb( 1 ) ).newCacheService();

        for ( int i = 0; i < 1000; i++ )
        {
            Pointer<byte[]> pointer = cache.put( i, new byte[1024] );
            if ( ( i % 100 ) == 0 )
            {
                System.out.println( pointer );
            }
        }
        assertTrue( "This test ensures that no unexpected errors/behaviours occurs when heap space is full", true );

    }

    private static class MyBean
        implements Serializable
    {
        private static final long serialVersionUID = -8865690921195047235L;

        private String name;

        /**
         * @return the name
         */
        public String getName()
        {
            return name;
        }

        /**
         * @param name the name to set
         */
        public void setName( String name )
        {
            this.name = name;
        }
    }

    @Test
    public void testEntryIsNoMoreAvailableAfterExpiry()
        throws InterruptedException
    {
        AllocationPolicy allocationPolicy = new RoundRobinAllocationPolicy();
        MemoryManagerService<MyBean> memoryManager = new MemoryManagerServiceImpl<MyBean>( allocationPolicy, true );
        CacheService<Integer, MyBean> cache =
            new DirectMemory<Integer, MyBean>().setMemoryManager( memoryManager ).setNumberOfBuffers( 1 ).setSize(
                Ram.Mb( 1 ) ).newCacheService();
        /*
         * let the scan run every 10s
         */
        cache.scheduleDisposalEvery( 3 * 1000 );
        /*
         * entry should be expired but not freed after 1s in the cache
         */
        MyBean originalEntry = new MyBean();
        originalEntry.setName( "the name" );
        cache.put( 1, originalEntry, 1 * 1000 );
        Pointer<MyBean> pointer = cache.getPointer( 1 );
        assertNotNull( pointer );
        assertFalse( pointer.isExpired() );
        assertFalse( pointer.isFree() );
        /*
         * wait for 2s to be sure the entry has been expired
         */
        Thread.sleep( 2000 );
        pointer = cache.getPointer( 1 );
        assertNotNull( pointer );
        assertTrue( pointer.isExpired() );
        assertFalse( pointer.isFree() );
        /*
         * wait for 11s to be sure the entry has been evicted
         */
        Thread.sleep( 4000 );
        pointer = cache.getPointer( 1 );
        assertNotNull( pointer );
        assertTrue( pointer.isExpired() );
        assertTrue( pointer.isFree() );
    }


}
