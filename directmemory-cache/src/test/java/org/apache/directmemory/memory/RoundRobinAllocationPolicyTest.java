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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

/**
 * Unit test of {@link RoundRobinAllocationPolicy} class.
 *
 * @author benoit@noisette.ch
 *
 */
public class RoundRobinAllocationPolicyTest
{

    private static final int NUMBER_OF_BUFFERS = 4;

    List<OffHeapMemoryBuffer> buffers;

    RoundRobinAllocationPolicy allocationPolicy;

    @Before
    public void initAllocationPolicy()
    {

        buffers = new ArrayList<OffHeapMemoryBuffer>();

        for ( int i = 0; i < NUMBER_OF_BUFFERS; i++ )
        {
            buffers.add( new DummyOffHeapMemoryBufferImpl() );
        }

        allocationPolicy = new RoundRobinAllocationPolicy();
        allocationPolicy.init( buffers );
    }

    @Test
    public void testSequence()
    {

        assertEquals( buffers.get( 0 ), allocationPolicy.getActiveBuffer( null, 1 ) );
        assertEquals( buffers.get( 1 ), allocationPolicy.getActiveBuffer( null, 1 ) );
        assertEquals( buffers.get( 2 ), allocationPolicy.getActiveBuffer( null, 1 ) );
        assertEquals( buffers.get( 3 ), allocationPolicy.getActiveBuffer( null, 1 ) );
        assertEquals( buffers.get( 0 ), allocationPolicy.getActiveBuffer( null, 1 ) );
        assertEquals( buffers.get( 1 ), allocationPolicy.getActiveBuffer( null, 1 ) );
        assertEquals( buffers.get( 2 ), allocationPolicy.getActiveBuffer( null, 1 ) );
        assertEquals( buffers.get( 3 ), allocationPolicy.getActiveBuffer( null, 1 ) );

        assertNotNull( allocationPolicy.getActiveBuffer( null, 1 ) );
        assertNotNull( allocationPolicy.getActiveBuffer( null, 2 ) );
        assertNull( allocationPolicy.getActiveBuffer( null, 3 ) );

        allocationPolicy.reset();

        assertEquals( buffers.get( 0 ), allocationPolicy.getActiveBuffer( null, 1 ) );
        assertEquals( buffers.get( 1 ), allocationPolicy.getActiveBuffer( null, 1 ) );
        assertEquals( buffers.get( 2 ), allocationPolicy.getActiveBuffer( null, 1 ) );
        assertEquals( buffers.get( 3 ), allocationPolicy.getActiveBuffer( null, 1 ) );
        assertEquals( buffers.get( 0 ), allocationPolicy.getActiveBuffer( null, 1 ) );
        assertEquals( buffers.get( 1 ), allocationPolicy.getActiveBuffer( null, 1 ) );
        assertEquals( buffers.get( 2 ), allocationPolicy.getActiveBuffer( null, 1 ) );
        assertEquals( buffers.get( 3 ), allocationPolicy.getActiveBuffer( null, 1 ) );

    }


    @Test
    public void testMaxAllocation()
    {

        allocationPolicy.setMaxAllocations( 1 );

        assertNotNull( allocationPolicy.getActiveBuffer( null, 1 ) );
        assertNull( allocationPolicy.getActiveBuffer( null, 2 ) );
        assertNull( allocationPolicy.getActiveBuffer( null, 3 ) );

    }


    /**
     * Dummy {@link OffHeapMemoryBuffer} that do nothing.
     */
    private static class DummyOffHeapMemoryBufferImpl
        implements OffHeapMemoryBuffer
    {

        @Override
        public int used()
        {
            return 0;
        }

        @Override
        public int capacity()
        {
            return 0;
        }

        @Override
        public int getBufferNumber()
        {
            return 0;
        }

        @Override
        public Pointer store( byte[] payload )
        {
            return null;
        }

        @Override
        public Pointer store( byte[] payload, Date expires )
        {
            return null;
        }

        @Override
        public Pointer store( byte[] payload, long expiresIn )
        {
            return null;
        }

        @Override
        public byte[] retrieve( Pointer pointer )
        {
            return null;
        }

        @Override
        public int free( Pointer pointer2free )
        {
            return 0;
        }

        @Override
        public void clear()
        {
        }

        @Override
        public void disposeExpiredRelative()
        {
        }

        @Override
        public void disposeExpiredAbsolute()
        {
        }

        @Override
        public long collectExpired()
        {
            return 0;
        }

        @Override
        public long collectLFU( int limit )
        {
            return 0;
        }

        @Override
        public Pointer update( Pointer pointer, byte[] payload )
        {
            return null;
        }

        @Override
        public Pointer allocate( int size, long expiresIn, long expires )
        {
            return null;
        }
    }
}
