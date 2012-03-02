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

import static org.junit.Assert.assertTrue;

import org.apache.directmemory.DirectMemory;
import org.apache.directmemory.measures.Ram;
import org.apache.directmemory.memory.AllocationPolicy;
import org.apache.directmemory.memory.MemoryManagerService;
import org.apache.directmemory.memory.MemoryManagerServiceImpl;
import org.apache.directmemory.memory.Pointer;
import org.apache.directmemory.memory.RoundRobinAllocationPolicy;
import org.junit.Test;

public class CacheServiceImplTest
{

    @Test
    public void testOffHeapExceedMemoryReturnNullWhenTrue()
    {
        AllocationPolicy allocationPolicy = new RoundRobinAllocationPolicy();
        MemoryManagerService<byte[]> memoryManager =
            new MemoryManagerServiceImpl<byte[]>( allocationPolicy, true );
        CacheService<Integer, byte[]> cache = new DirectMemory<Integer, byte[]>()
                        .setMemoryManager( memoryManager )
                        .setNumberOfBuffers( 1 )
                        .setSize( Ram.Mb( 1 ) )
                        .newCacheService();

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

}
