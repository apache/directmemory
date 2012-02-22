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

import java.util.Random;

import junit.framework.Assert;

import org.apache.directmemory.memory.MemoryManagerService;
import org.apache.directmemory.memory.MemoryManagerServiceImpl;
import org.apache.directmemory.memory.Pointer;
import org.junit.Test;

public class MemoryManagerServiceImplTest
{

    protected static final Random R = new Random();

    protected static final byte[] SMALL_PAYLOAD = "ABCD".getBytes();

    protected MemoryManagerService<Object> getMemoryManagerService()
    {
        return new MemoryManagerServiceImpl<Object>();
    }

    @Test
    public void testFirstMatchBorderCase()
    {

        // Storing a first payload of 4 bytes, 1 byte remaining in the buffer.
        // When storing a second 4 bytes payload, an BufferOverflowException is
        // thrown instead of an easy check.

        final int BUFFER_SIZE = 5;

        final MemoryManagerService<Object> memoryManagerService = getMemoryManagerService();

        memoryManagerService.init( 1, BUFFER_SIZE );

        Pointer<Object> pointer1 = memoryManagerService.store( SMALL_PAYLOAD );
        Assert.assertNotNull( pointer1 );

        Pointer<Object> pointer2 = memoryManagerService.store( SMALL_PAYLOAD );
        Assert.assertNull( pointer2 );

    }

    @Test
    public void testAllocateMultipleBuffers()
    {

        // Initializing 4 buffers of 4 bytes, MemoryManagerService should search
        // for available space in another buffer.

        final int NUMBER_OF_OBJECTS = 4;

        final MemoryManagerService<Object> memoryManagerService = getMemoryManagerService();

        memoryManagerService.init( NUMBER_OF_OBJECTS, SMALL_PAYLOAD.length );

        for ( int i = 0; i < NUMBER_OF_OBJECTS; i++ )
        {
            Pointer<Object> pointer = memoryManagerService.store( SMALL_PAYLOAD );
            Assert.assertNotNull( pointer );
        }

        Pointer<Object> pointerNull = memoryManagerService.store( SMALL_PAYLOAD );
        Assert.assertNull( pointerNull );
    }

    @Test
    public void testByteLeaking()
    {

        // Initializing 1 buffer of 10*4 bytes, should be able to allocate 10
        // objects of 4 bytes.

        final int NUMBER_OF_OBJECTS = 10;

        final MemoryManagerService<Object> memoryManagerService = getMemoryManagerService();
        memoryManagerService.init( 1, NUMBER_OF_OBJECTS * SMALL_PAYLOAD.length );

        for ( int i = 0; i < NUMBER_OF_OBJECTS; i++ )
        {
            Pointer<Object> pointer = memoryManagerService.store( SMALL_PAYLOAD );
            Assert.assertNotNull( pointer );
        }

        Pointer<Object> pointerNull = memoryManagerService.store( SMALL_PAYLOAD );
        Assert.assertNull( pointerNull );
    }

    @Test
    public void testReportCorrectUsedMemory()
    {

        // Initializing 1 buffer of 4*4 bytes, storing and freeing and storing
        // again should report correct numbers.

        final int NUMBER_OF_OBJECTS = 4;
        final int BUFFER_SIZE = NUMBER_OF_OBJECTS * SMALL_PAYLOAD.length;

        final MemoryManagerService<Object> memoryManagerService = getMemoryManagerService();

        memoryManagerService.init( 1, BUFFER_SIZE );

        Pointer<Object> lastPointer = null;
        for ( int i = 0; i < NUMBER_OF_OBJECTS; i++ )
        {
            Pointer<Object> pointer = memoryManagerService.store( SMALL_PAYLOAD );
            Assert.assertNotNull( pointer );
            lastPointer = pointer;
        }

        // Buffer is fully used.
        Assert.assertEquals( BUFFER_SIZE, memoryManagerService.getBuffers().get( 0 ).used() );

        Assert.assertNotNull( lastPointer );
        memoryManagerService.free( lastPointer );

        Pointer<Object> pointerNotNull = memoryManagerService.store( SMALL_PAYLOAD );
        Assert.assertNotNull( pointerNotNull );

        // Buffer again fully used.
        Assert.assertEquals( BUFFER_SIZE, memoryManagerService.getBuffers().get( 0 ).used() );

    }

    @Test
    public void testRandomPayload()
    {

        final int NUMBER_OF_OBJECTS = 10;
        final int BUFFER_SIZE = NUMBER_OF_OBJECTS * SMALL_PAYLOAD.length;

        final MemoryManagerService<Object> memoryManagerService = getMemoryManagerService();

        memoryManagerService.init( 1, BUFFER_SIZE );

        for ( int i = 0; i < NUMBER_OF_OBJECTS; i++ )
        {
            byte[] payload = MemoryTestUtils.generateRandomPayload( SMALL_PAYLOAD.length );
            Pointer<Object> pointer = memoryManagerService.store( payload );
            Assert.assertNotNull( pointer );
            byte[] fetchedPayload = memoryManagerService.retrieve( pointer );
            Assert.assertEquals( new String( payload ), new String( fetchedPayload ) );
            if ( R.nextBoolean() )
            {
                memoryManagerService.free( pointer );
            }
        }

        memoryManagerService.clear();

        for ( int i = 0; i < NUMBER_OF_OBJECTS; i++ )
        {
            byte[] payload = MemoryTestUtils.generateRandomPayload( SMALL_PAYLOAD.length );
            Pointer<Object> pointer = memoryManagerService.store( payload );
            Assert.assertNotNull( pointer );
            byte[] fetchedPayload = memoryManagerService.retrieve( pointer );
            Assert.assertEquals( new String( payload ), new String( fetchedPayload ) );
            if ( R.nextBoolean() )
            {
                memoryManagerService.free( pointer );
                i--;
            }
        }

        memoryManagerService.clear();

        Pointer<Object> pointer = null;
        do
        {
            byte[] payload = MemoryTestUtils.generateRandomPayload( R.nextInt( BUFFER_SIZE / 4 + 1 ) );
            pointer = memoryManagerService.store( payload );
            if ( pointer != null && R.nextBoolean() )
            {
                memoryManagerService.free( pointer );
            }
        }
        while ( pointer != null );

    }

}
