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

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.directmemory.memory.MemoryManagerService;
import org.apache.directmemory.memory.MemoryManagerServiceWithAllocationPolicyImpl;
import org.apache.directmemory.memory.OffHeapMemoryBuffer;
import org.apache.directmemory.memory.OffHeapMergingMemoryBufferImpl;
import org.apache.directmemory.memory.Pointer;
import org.apache.directmemory.memory.RoundRobinAllocationPolicy;
import org.junit.Test;

public class MemoryManagerServiceImplWithMerginOHMBAndAllocationPolicyTest
    extends MemoryManagerServiceImplTest
{

    @Override
    protected MemoryManagerService<Object> getMemoryManagerService()
    {
        // Initialize MemoryManagerService with OffHeapMergingMemoryBufferImpl and AllocationPolicy
        final MemoryManagerServiceWithAllocationPolicyImpl<Object> mms = new MemoryManagerServiceWithAllocationPolicyImpl<Object>()
        {
            @Override
            protected OffHeapMemoryBuffer<Object> instanciateOffHeapMemoryBuffer( int size, int bufferNumber )
            {
                return OffHeapMergingMemoryBufferImpl.createNew( size, bufferNumber );
            }
        };

        mms.setAllocationPolicy( new RoundRobinAllocationPolicy<Object>() );
        return mms;
    }

    @Test
    public void testFullFillAndClearBuffer()
    {

        final int NUMBER_OF_OBJECTS = 10;
        final int BUFFER_SIZE = NUMBER_OF_OBJECTS * SMALL_PAYLOAD.length;

        final MemoryManagerService<Object> memoryManagerService = getMemoryManagerService();

        memoryManagerService.init( 1, BUFFER_SIZE );

        Pointer<Object> pointerFull = memoryManagerService.store( MemoryTestUtils.generateRandomPayload( BUFFER_SIZE ) );
        Assert.assertNotNull( pointerFull );
        memoryManagerService.free( pointerFull );

        final int size1 = R.nextInt( BUFFER_SIZE / 2 ) + 1;
        Pointer<Object> pointer1 = memoryManagerService.store( MemoryTestUtils.generateRandomPayload( size1 ) );
        Assert.assertNotNull( "Cannot store " + size1 + " bytes", pointer1 );

        final int size2 = R.nextInt( ( BUFFER_SIZE - size1 ) / 2 ) + 1;
        Pointer<Object> pointer2 = memoryManagerService.store( MemoryTestUtils.generateRandomPayload( size2 ) );
        Assert.assertNotNull( "Cannot store " + size2 + " bytes", pointer2 );

        final int size3 = R.nextInt( ( BUFFER_SIZE - size1 - size2 ) / 2 ) + 1;
        Pointer<Object> pointer3 = memoryManagerService.store( MemoryTestUtils.generateRandomPayload( size3 ) );
        Assert.assertNotNull( "Cannot store " + size3 + " bytes", pointer3 );

        final int size4 = BUFFER_SIZE - size1 - size2 - size3;
        Pointer<Object> pointer4 = memoryManagerService.store( MemoryTestUtils.generateRandomPayload( size4 ) );
        Assert.assertNotNull( "Cannot store " + size4 + " bytes", pointer4 );

        memoryManagerService.free( pointer1 );

        memoryManagerService.free( pointer3 );

        memoryManagerService.free( pointer4 );

        memoryManagerService.free( pointer2 );

        Assert.assertEquals( 0, memoryManagerService.getBuffers().get( 0 ).used() );

        // As all pointers have been freeed, we should be able to reallocate the
        // whole buffer
        Pointer<Object> pointer6 = memoryManagerService.store( MemoryTestUtils.generateRandomPayload( BUFFER_SIZE ) );
        Assert.assertNotNull( "Cannot store " + BUFFER_SIZE + " bytes", pointer6 );

        memoryManagerService.clear();

        // As all pointers have been cleared, we should be able to reallocate
        // the whole buffer
        Pointer<Object> pointer7 = memoryManagerService.store( MemoryTestUtils.generateRandomPayload( BUFFER_SIZE ) );
        Assert.assertNotNull( "Cannot store " + BUFFER_SIZE + " bytes", pointer7 );

        memoryManagerService.clear();

        // As all pointers have been cleared, we should be able to reallocate
        // the whole buffer
        for ( int i = 0; i < NUMBER_OF_OBJECTS; i++ )
        {
            Pointer<Object> pointer = memoryManagerService.store( SMALL_PAYLOAD );
            Assert.assertNotNull( pointer );
        }

        memoryManagerService.clear();

        // As all pointers have been cleared, we should be able to reallocate
        // the whole buffer
        Pointer<Object> pointer8 = memoryManagerService.store( MemoryTestUtils.generateRandomPayload( BUFFER_SIZE ) );
        Assert.assertNotNull( "Cannot store " + BUFFER_SIZE + " bytes", pointer8 );

        memoryManagerService.free( pointer8 );

        // As all pointers have been cleared, we should be able to reallocate
        // the whole buffer
        for ( int i = 0; i < NUMBER_OF_OBJECTS * 10; i++ )
        {
            Pointer<Object> pointer = memoryManagerService.store( SMALL_PAYLOAD );
            Assert.assertNotNull( pointer );
            memoryManagerService.free( pointer );
        }

    }

    @Test
    public void testStoreAllocAndFree()
    {

        final int NUMBER_OF_OBJECTS = 100;
        final int BUFFER_SIZE = NUMBER_OF_OBJECTS * SMALL_PAYLOAD.length;

        final MemoryManagerService<Object> memoryManagerService = getMemoryManagerService();

        memoryManagerService.init( 1, BUFFER_SIZE );

        List<Pointer<Object>> pointers = new ArrayList<Pointer<Object>>( NUMBER_OF_OBJECTS );
        for ( int i = 0; i < NUMBER_OF_OBJECTS; i++ )
        {
            byte[] payload = MemoryTestUtils.generateRandomPayload( SMALL_PAYLOAD.length );
            Pointer<Object> pointer = memoryManagerService.store( payload );
            Assert.assertNotNull( pointer );
            pointers.add( pointer );
            byte[] fetchedPayload = memoryManagerService.retrieve( pointer );
            Assert.assertEquals( new String( payload ), new String( fetchedPayload ) );
        }

        // Free 1/4 of the pointers, from 1/4 of the address space to 1/2
        for ( int i = NUMBER_OF_OBJECTS / 4; i < NUMBER_OF_OBJECTS / 2; i++ )
        {
            Pointer<Object> pointer = pointers.get( i );
            memoryManagerService.free( pointer );
        }

        // Should be able to allocate NUMBER_OF_OBJECTS / 4 *
        // SMALL_PAYLOAD.length bytes
        Pointer<Object> pointer1 = memoryManagerService.allocate( Object.class, NUMBER_OF_OBJECTS / 4 * SMALL_PAYLOAD.length, 0, 0 );
        Assert.assertNotNull( "Cannot store " + ( NUMBER_OF_OBJECTS / 4 * SMALL_PAYLOAD.length ) + " bytes", pointer1 );

        int pointerToSkip = NUMBER_OF_OBJECTS / 2 + NUMBER_OF_OBJECTS / 10;
        for ( int i = NUMBER_OF_OBJECTS / 2; i < NUMBER_OF_OBJECTS * 3 / 4; i++ )
        {
            // skip one pointer
            if ( i == pointerToSkip )
            {
                continue;
            }
            Pointer<Object> pointer = pointers.get( i );
            memoryManagerService.free( pointer );
        }

        // Should NOT be able to allocate NUMBER_OF_OBJECTS / 4 *
        // SMALL_PAYLOAD.length bytes
        Pointer<Object> pointer2 = memoryManagerService.allocate( Object.class, NUMBER_OF_OBJECTS / 4 * SMALL_PAYLOAD.length, 0, 0 );
        Assert.assertNull( pointer2 );

        // Freeing the previously skipped pointer should then merge the whole
        // memory space
        memoryManagerService.free( pointers.get( pointerToSkip ) );

        // Should be able to allocate NUMBER_OF_OBJECTS / 4 *
        // SMALL_PAYLOAD.length bytes
        Pointer<Object> pointer3 = memoryManagerService.allocate( Object.class, NUMBER_OF_OBJECTS / 4 * SMALL_PAYLOAD.length, 0, 0 );
        Assert.assertNotNull( pointer3 );

        byte[] payload3 = MemoryTestUtils.generateRandomPayload( NUMBER_OF_OBJECTS / 4 * SMALL_PAYLOAD.length );
        pointer3.getDirectBuffer().put( payload3 );
        byte[] retrievePayload3 = memoryManagerService.retrieve( pointer3 );
        Assert.assertEquals( new String( payload3 ), new String( retrievePayload3 ) );

    }

}
