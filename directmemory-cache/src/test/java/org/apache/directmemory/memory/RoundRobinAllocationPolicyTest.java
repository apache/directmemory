package org.apache.directmemory.memory;

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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.directmemory.memory.allocator.Allocator;
import org.apache.directmemory.memory.buffer.MemoryBuffer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test of {@link RoundRobinAllocationPolicy} class.
 * 
 * @author benoit@noisette.ch
 */
public class RoundRobinAllocationPolicyTest
{

    private static final int NUMBER_OF_BUFFERS = 4;

    List<Allocator> allocators;

    RoundRobinAllocationPolicy allocationPolicy;

    @Before
    public void initAllocationPolicy()
    {

        allocators = new ArrayList<Allocator>();

        for ( int i = 0; i < NUMBER_OF_BUFFERS; i++ )
        {
            allocators.add( new DummyByteBufferAllocator() );
        }

        allocationPolicy = new RoundRobinAllocationPolicy();
        allocationPolicy.init( allocators );
    }

    @After
    public void cleanup()
        throws IOException
    {
        for ( Allocator allocator : allocators )
        {
            allocator.close();
        }
    }

    @Test
    public void testSequence()
    {

        assertEquals( allocators.get( 0 ), allocationPolicy.getActiveAllocator( null, 1 ) );
        assertEquals( allocators.get( 1 ), allocationPolicy.getActiveAllocator( null, 1 ) );
        assertEquals( allocators.get( 2 ), allocationPolicy.getActiveAllocator( null, 1 ) );
        assertEquals( allocators.get( 3 ), allocationPolicy.getActiveAllocator( null, 1 ) );
        assertEquals( allocators.get( 0 ), allocationPolicy.getActiveAllocator( null, 1 ) );
        assertEquals( allocators.get( 1 ), allocationPolicy.getActiveAllocator( null, 1 ) );
        assertEquals( allocators.get( 2 ), allocationPolicy.getActiveAllocator( null, 1 ) );
        assertEquals( allocators.get( 3 ), allocationPolicy.getActiveAllocator( null, 1 ) );

        assertNotNull( allocationPolicy.getActiveAllocator( null, 1 ) );
        assertNotNull( allocationPolicy.getActiveAllocator( null, 2 ) );
        assertNull( allocationPolicy.getActiveAllocator( null, 3 ) );

        allocationPolicy.reset();

        assertEquals( allocators.get( 0 ), allocationPolicy.getActiveAllocator( null, 1 ) );
        assertEquals( allocators.get( 1 ), allocationPolicy.getActiveAllocator( null, 1 ) );
        assertEquals( allocators.get( 2 ), allocationPolicy.getActiveAllocator( null, 1 ) );
        assertEquals( allocators.get( 3 ), allocationPolicy.getActiveAllocator( null, 1 ) );
        assertEquals( allocators.get( 0 ), allocationPolicy.getActiveAllocator( null, 1 ) );
        assertEquals( allocators.get( 1 ), allocationPolicy.getActiveAllocator( null, 1 ) );
        assertEquals( allocators.get( 2 ), allocationPolicy.getActiveAllocator( null, 1 ) );
        assertEquals( allocators.get( 3 ), allocationPolicy.getActiveAllocator( null, 1 ) );

    }

    @Test
    public void testMaxAllocation()
    {

        allocationPolicy.setMaxAllocations( 1 );

        assertNotNull( allocationPolicy.getActiveAllocator( null, 1 ) );
        assertNull( allocationPolicy.getActiveAllocator( null, 2 ) );
        assertNull( allocationPolicy.getActiveAllocator( null, 3 ) );

    }

    /**
     * Dummy {@link OffHeapMemoryBuffer} that do nothing.
     */
    private static class DummyByteBufferAllocator
        implements Allocator
    {

        @Override
        public void free( MemoryBuffer buffer )
        {
        }

        @Override
        public MemoryBuffer allocate( int size )
        {
            return null;
        }

        @Override
        public void clear()
        {
        }

        @Override
        public int getCapacity()
        {
            return 0;
        }

        @Override
        public int getNumber()
        {
            return 0;
        }

        @Override
        public void close()
            throws IOException
        {

        }

    }
}
