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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import junit.framework.Assert;

import org.apache.directmemory.memory.MemoryManagerService;
import org.apache.directmemory.memory.OffHeapMergingMemoryBufferImpl;
import org.apache.directmemory.memory.OffHeapMemoryBuffer;
import org.apache.directmemory.memory.Pointer;
import org.junit.Test;

public abstract class AbstractOffHeapMemoryBufferTest
{

    protected static final Random R = new Random();

    protected static final int SMALL_PAYLOAD_LENGTH = 4;
    protected static final byte[] SMALL_PAYLOAD = MemoryTestUtils.generateRandomPayload( SMALL_PAYLOAD_LENGTH );

    
    protected abstract OffHeapMemoryBuffer instanciateOffHeapMemoryBuffer( int bufferSize );
    
    /**
     * Test pointers allocation, when buffer size is not aligned with the size of stored objects.
     * Null {@link Pointer} should be returned to allow {@link MemoryManagerService} to go to next step with allocation policy.
     */
    @Test
    public void testNotEnoughFreeSpace()
    {

        // Storing a first payload of 4 bytes, 1 byte remaining in the buffer. When storing a second 4 bytes payload, an null pointer should be returned.

        final int BUFFER_SIZE = SMALL_PAYLOAD_LENGTH + 1;

        final OffHeapMemoryBuffer offHeapMemoryBuffer = instanciateOffHeapMemoryBuffer( BUFFER_SIZE );

        Pointer pointer1 = offHeapMemoryBuffer.store( SMALL_PAYLOAD );
        Assert.assertNotNull( pointer1 );
        Assert.assertFalse( pointer1.free );
        Assert.assertNull( pointer1.directBuffer );

        Pointer pointer2 = offHeapMemoryBuffer.store( SMALL_PAYLOAD );
        Assert.assertNull( pointer2 );

    }
    

    /**
     * Ensure no byte is leaking when allocating several objects.
     */
    @Test
    public void testByteLeaking()
    {

        // Initializing 1 buffer of 10*4 bytes, should be able to allocate 10 objects of 4 bytes.

        final int NUMBER_OF_OBJECTS = 10;
        final int BUFFER_SIZE = NUMBER_OF_OBJECTS * SMALL_PAYLOAD_LENGTH;
        
        final OffHeapMemoryBuffer offHeapMemoryBuffer = instanciateOffHeapMemoryBuffer( BUFFER_SIZE );

        for ( int i = 0; i < NUMBER_OF_OBJECTS; i++ )
        {
            Pointer pointer = offHeapMemoryBuffer.store( SMALL_PAYLOAD );
            Assert.assertNotNull( pointer );
        }

        Pointer pointerNull = offHeapMemoryBuffer.store( SMALL_PAYLOAD );
        Assert.assertNull( pointerNull );
    }

    /**
     * Ensure memory usage is reported correctly
     */
    @Test
    public void testReportCorrectUsedMemory()
    {

        // Initializing 1 buffer of 4*4 bytes, storing and freeing and storing again should report correct numbers.

        final int NUMBER_OF_OBJECTS = 4;
        final int BUFFER_SIZE = NUMBER_OF_OBJECTS * SMALL_PAYLOAD_LENGTH;

        final OffHeapMemoryBuffer offHeapMemoryBuffer = instanciateOffHeapMemoryBuffer( BUFFER_SIZE );

        Pointer lastPointer = null;
        for ( int i = 0; i < NUMBER_OF_OBJECTS; i++ )
        {
            Pointer pointer = offHeapMemoryBuffer.store( SMALL_PAYLOAD );
            Assert.assertNotNull( pointer );
            lastPointer = pointer;
        }

        // Buffer is fully used.
        Assert.assertEquals( BUFFER_SIZE, offHeapMemoryBuffer.used() );

        Assert.assertNotNull( lastPointer );
        offHeapMemoryBuffer.free( lastPointer );

        Pointer pointerNotNull = offHeapMemoryBuffer.store( SMALL_PAYLOAD );
        Assert.assertNotNull( pointerNotNull );

        // Buffer again fully used.
        Assert.assertEquals( BUFFER_SIZE, offHeapMemoryBuffer.used() );

    }

    /**
     * Completely fill the buffer, free some pointer, reallocated the freed space, clear the buffer. The entire space should be  
     */
    @Test
    public void testFullFillAndFreeAndClearBuffer()
    {

        final int NUMBER_OF_OBJECTS = 10;
        final int BUFFER_SIZE = NUMBER_OF_OBJECTS * SMALL_PAYLOAD_LENGTH;

        final OffHeapMemoryBuffer offHeapMemoryBuffer = instanciateOffHeapMemoryBuffer( BUFFER_SIZE );

        Pointer pointerFull = offHeapMemoryBuffer.store( MemoryTestUtils.generateRandomPayload( BUFFER_SIZE ) );
        Assert.assertNotNull( pointerFull );
        offHeapMemoryBuffer.free( pointerFull );

        final int size1 = R.nextInt( BUFFER_SIZE / 2 ) + 1;
        Pointer pointer1 = offHeapMemoryBuffer.store( MemoryTestUtils.generateRandomPayload( size1 ) );
        Assert.assertNotNull( "Cannot store " + size1 + " bytes", pointer1 );

        final int size2 = R.nextInt( ( BUFFER_SIZE - size1 ) / 2 ) + 1;
        Pointer pointer2 = offHeapMemoryBuffer.store( MemoryTestUtils.generateRandomPayload( size2 ) );
        Assert.assertNotNull( "Cannot store " + size2 + " bytes", pointer2 );

        final int size3 = R.nextInt( ( BUFFER_SIZE - size1 - size2 ) / 2 ) + 1;
        Pointer pointer3 = offHeapMemoryBuffer.store( MemoryTestUtils.generateRandomPayload( size3 ) );
        Assert.assertNotNull( "Cannot store " + size3 + " bytes", pointer3 );

        final int size4 = BUFFER_SIZE - size1 - size2 - size3;
        Pointer pointer4 = offHeapMemoryBuffer.store( MemoryTestUtils.generateRandomPayload( size4 ) );
        Assert.assertNotNull( "Cannot store " + size4 + " bytes", pointer4 );

        offHeapMemoryBuffer.free( pointer1 );
        Assert.assertTrue( pointer1.free );
        
        offHeapMemoryBuffer.free( pointer3 );

        offHeapMemoryBuffer.free( pointer4 );

        offHeapMemoryBuffer.free( pointer2 );

        Assert.assertEquals( 0, offHeapMemoryBuffer.used() );

        // As all pointers have been freeed, we should be able to reallocate the whole buffer
        Pointer pointer6 = offHeapMemoryBuffer.store( MemoryTestUtils.generateRandomPayload( BUFFER_SIZE ) );
        Assert.assertNotNull( "Cannot store " + BUFFER_SIZE + " bytes", pointer6 );

        offHeapMemoryBuffer.clear();

        // As all pointers have been cleared, we should be able to reallocate the whole buffer
        Pointer pointer7 = offHeapMemoryBuffer.store( MemoryTestUtils.generateRandomPayload( BUFFER_SIZE ) );
        Assert.assertNotNull( "Cannot store " + BUFFER_SIZE + " bytes", pointer7 );

        offHeapMemoryBuffer.clear();

        // As all pointers have been cleared, we should be able to reallocate the whole buffer
        for ( int i = 0; i < NUMBER_OF_OBJECTS; i++ )
        {
            Pointer pointer = offHeapMemoryBuffer.store( SMALL_PAYLOAD );
            Assert.assertNotNull( pointer );
        }

        offHeapMemoryBuffer.clear();

        // As all pointers have been cleared, we should be able to reallocate the whole buffer
        Pointer pointer8 = offHeapMemoryBuffer.store( MemoryTestUtils.generateRandomPayload( BUFFER_SIZE ) );
        Assert.assertNotNull( "Cannot store " + BUFFER_SIZE + " bytes", pointer8 );

        offHeapMemoryBuffer.free( pointer8 );

        // As all pointers have been cleared, we should be able to reallocate the whole buffer
        for ( int i = 0; i < NUMBER_OF_OBJECTS * 10; i++ )
        {
            Pointer pointer = offHeapMemoryBuffer.store( SMALL_PAYLOAD );
            Assert.assertNotNull( pointer );
            offHeapMemoryBuffer.free( pointer );
        }

        // After a clear occurs, pointers allocated before the clear should be set as "free"
        Assert.assertTrue( pointer6.free );
        Assert.assertTrue( pointer7.free );
        
    }

    @Test
    public void testRandomPayload()
    {

        final int NUMBER_OF_OBJECTS = 10;
        final int BUFFER_SIZE = NUMBER_OF_OBJECTS * SMALL_PAYLOAD_LENGTH;

        final OffHeapMemoryBuffer offHeapMemoryBuffer = instanciateOffHeapMemoryBuffer( BUFFER_SIZE );

        for ( int i = 0; i < NUMBER_OF_OBJECTS; i++ )
        {
            byte[] payload = MemoryTestUtils.generateRandomPayload( SMALL_PAYLOAD_LENGTH );
            Pointer pointer = offHeapMemoryBuffer.store( payload );
            Assert.assertNotNull( pointer );
            byte[] fetchedPayload = offHeapMemoryBuffer.retrieve( pointer );
            Assert.assertEquals( new String( payload ), new String( fetchedPayload ) );
            if ( R.nextBoolean() )
            {
                offHeapMemoryBuffer.free( pointer );
            }
        }

        offHeapMemoryBuffer.clear();

        for ( int i = 0; i < NUMBER_OF_OBJECTS; i++ )
        {
            byte[] payload = MemoryTestUtils.generateRandomPayload( SMALL_PAYLOAD_LENGTH );
            Pointer pointer = offHeapMemoryBuffer.store( payload );
            Assert.assertNotNull( pointer );
            byte[] fetchedPayload = offHeapMemoryBuffer.retrieve( pointer );
            Assert.assertEquals( new String( payload ), new String( fetchedPayload ) );
            if ( R.nextBoolean() )
            {
                offHeapMemoryBuffer.free( pointer );
                i--;
            }
        }

    }

    @Test
    public void testStoreAllocAndFree()
    {

        final int NUMBER_OF_OBJECTS = 100;
        final int BUFFER_SIZE = NUMBER_OF_OBJECTS * SMALL_PAYLOAD_LENGTH;

        final OffHeapMemoryBuffer offHeapMemoryBuffer = instanciateOffHeapMemoryBuffer( BUFFER_SIZE );

        List<Pointer> pointers = new ArrayList<Pointer>( NUMBER_OF_OBJECTS );
        for ( int i = 0; i < NUMBER_OF_OBJECTS; i++ )
        {
            byte[] payload = MemoryTestUtils.generateRandomPayload( SMALL_PAYLOAD_LENGTH );
            Pointer pointer = offHeapMemoryBuffer.store( payload );
            Assert.assertNotNull( pointer );
            pointers.add( pointer );
            byte[] fetchedPayload = offHeapMemoryBuffer.retrieve( pointer );
            Assert.assertEquals( new String( payload ), new String( fetchedPayload ) );
        }

        // Free 1/4 of the pointers, from 1/4 of the address space to 1/2
        for ( int i = NUMBER_OF_OBJECTS / 4; i < NUMBER_OF_OBJECTS / 2; i++ )
        {
            Pointer pointer = pointers.get( i );
            offHeapMemoryBuffer.free( pointer );
        }

        // Should be able to allocate NUMBER_OF_OBJECTS / 4 * SMALL_PAYLOAD_LENGTH bytes
        Pointer pointer1 = offHeapMemoryBuffer.allocate( NUMBER_OF_OBJECTS / 4 * SMALL_PAYLOAD_LENGTH, 0, 0 );
        Assert.assertNotNull( pointer1 );

        int pointerToSkip = NUMBER_OF_OBJECTS / 2 + NUMBER_OF_OBJECTS / 10;
        for ( int i = NUMBER_OF_OBJECTS / 2; i < NUMBER_OF_OBJECTS * 3 / 4; i++ )
        {
            // skip one pointer 
            if ( i == pointerToSkip )
            {
                continue;
            }
            Pointer pointer = pointers.get( i );
            offHeapMemoryBuffer.free( pointer );
        }

        // Should NOT be able to allocate NUMBER_OF_OBJECTS / 4 * SMALL_PAYLOAD_LENGTH bytes
        Pointer pointer2 = offHeapMemoryBuffer.allocate( NUMBER_OF_OBJECTS / 4 * SMALL_PAYLOAD_LENGTH, 0, 0 );
        Assert.assertNull( pointer2 );

        // Freeing the previously skipped pointer should then merge the whole memory space
        offHeapMemoryBuffer.free( pointers.get( pointerToSkip ) );

        // Should be able to allocate NUMBER_OF_OBJECTS / 4 * SMALL_PAYLOAD_LENGTH bytes
        Pointer pointer3 = offHeapMemoryBuffer.allocate( NUMBER_OF_OBJECTS / 4 * SMALL_PAYLOAD_LENGTH, 0, 0 );
        Assert.assertNotNull( pointer3 );

        byte[] payload3 = MemoryTestUtils.generateRandomPayload( NUMBER_OF_OBJECTS / 4 * SMALL_PAYLOAD_LENGTH );
        pointer3.directBuffer.put( payload3 );
        byte[] retrievePayload3 = offHeapMemoryBuffer.retrieve( pointer3 );
        Assert.assertEquals( new String( payload3 ), new String( retrievePayload3 ) );

    }

    @Test
    public void testRandomPayload2()
    {

        final int NUMBER_OF_OBJECTS = 10;
        final int BUFFER_SIZE = NUMBER_OF_OBJECTS * SMALL_PAYLOAD_LENGTH;

        final OffHeapMergingMemoryBufferImpl offHeapLinkedMemoryBuffer = OffHeapMergingMemoryBufferImpl
            .createNew( BUFFER_SIZE );

        byte[] payload1 = MemoryTestUtils.generateRandomPayload( 2 * SMALL_PAYLOAD_LENGTH );
        Pointer pointer1 = offHeapLinkedMemoryBuffer.store( payload1 );
        Assert.assertNotNull( pointer1 );

        byte[] fetchedPayload1 = offHeapLinkedMemoryBuffer.retrieve( pointer1 );
        Assert.assertEquals( new String( payload1 ), new String( fetchedPayload1 ) );

        byte[] payload2 = MemoryTestUtils.generateRandomPayload( SMALL_PAYLOAD_LENGTH );
        Pointer pointer2 = offHeapLinkedMemoryBuffer.store( payload2 );
        Assert.assertNotNull( pointer2 );

        byte[] fetchedPayload2 = offHeapLinkedMemoryBuffer.retrieve( pointer2 );
        Assert.assertEquals( new String( payload2 ), new String( fetchedPayload2 ) );

        offHeapLinkedMemoryBuffer.free( pointer1 );

        offHeapLinkedMemoryBuffer.free( pointer1 );

        byte[] payload3 = MemoryTestUtils.generateRandomPayload( SMALL_PAYLOAD_LENGTH );
        Pointer pointer3 = offHeapLinkedMemoryBuffer.store( payload3 );
        Assert.assertNotNull( pointer3 );

        byte[] fetchedPayload3 = offHeapLinkedMemoryBuffer.retrieve( pointer3 );
        Assert.assertEquals( new String( payload3 ), new String( fetchedPayload3 ) );

    }

    @Test
    public void testUpdate()
    {

        final int NUMBER_OF_OBJECTS = 1;
        final int BUFFER_SIZE = NUMBER_OF_OBJECTS * SMALL_PAYLOAD_LENGTH;

        final OffHeapMemoryBuffer offHeapMemoryBuffer = instanciateOffHeapMemoryBuffer( BUFFER_SIZE );

        final byte[] payload = MemoryTestUtils.generateRandomPayload( SMALL_PAYLOAD_LENGTH );

        final Pointer pointer = offHeapMemoryBuffer.store( payload );
        Assert.assertNotNull( pointer );
        Assert.assertEquals( new String( payload ), new String( offHeapMemoryBuffer.retrieve( pointer ) ) );

        final byte[] otherPayload = MemoryTestUtils.generateRandomPayload( SMALL_PAYLOAD_LENGTH );
        final Pointer otherPointer = offHeapMemoryBuffer.update( pointer, otherPayload );
        Assert.assertNotNull( otherPointer );
        Assert.assertEquals( pointer.start, otherPointer.start );
        Assert.assertEquals( pointer.end, otherPointer.end );
        Assert.assertEquals( new String( otherPayload ), new String( offHeapMemoryBuffer.retrieve( otherPointer ) ) );

        final byte[] evenAnotherPayload = MemoryTestUtils.generateRandomPayload( SMALL_PAYLOAD_LENGTH / 2 );
        final Pointer evenAnotherPointer = offHeapMemoryBuffer.update( pointer, evenAnotherPayload );
        Assert.assertNotNull( evenAnotherPointer );
        Assert.assertEquals( pointer.start, evenAnotherPointer.start );
        Assert.assertEquals( pointer.end, evenAnotherPointer.end );
        Assert.assertEquals( 4, new String( offHeapMemoryBuffer.retrieve( evenAnotherPointer ) ).length() );
        Assert.assertTrue( new String( offHeapMemoryBuffer.retrieve( evenAnotherPointer ) )
            .startsWith( new String( evenAnotherPayload ) ) );

    }
    
    
    @Test
    public void testAllocate()
    {

        final int NUMBER_OF_OBJECTS = 10;
        final int BUFFER_SIZE = NUMBER_OF_OBJECTS * SMALL_PAYLOAD_LENGTH;

        final OffHeapMemoryBuffer offHeapMemoryBuffer = instanciateOffHeapMemoryBuffer( BUFFER_SIZE );

        final byte[] payload1 = MemoryTestUtils.generateRandomPayload( 8 * SMALL_PAYLOAD_LENGTH );
        final Pointer pointer1 = offHeapMemoryBuffer.store( payload1 );
        Assert.assertNotNull( pointer1 );
        Assert.assertEquals( new String( payload1 ), new String( offHeapMemoryBuffer.retrieve( pointer1 ) ) );

        final byte[] payload2 = MemoryTestUtils.generateRandomPayload( 2 * SMALL_PAYLOAD_LENGTH );
        final Pointer pointer2 = offHeapMemoryBuffer.store( payload2 );
        Assert.assertNotNull( pointer2 );
        Assert.assertEquals( new String( payload2 ), new String( offHeapMemoryBuffer.retrieve( pointer2 ) ) );

        offHeapMemoryBuffer.free( pointer1 );
        
        final byte[] payload3 = MemoryTestUtils.generateRandomPayload( 2 * SMALL_PAYLOAD_LENGTH );
        final Pointer pointer3 = offHeapMemoryBuffer.store( payload3 );
        Assert.assertNotNull( pointer3 );
        Assert.assertEquals( new String( payload3 ), new String( offHeapMemoryBuffer.retrieve( pointer3 ) ) );

        final int size1 = 4 * SMALL_PAYLOAD_LENGTH;
        final byte[] allocatedPayload1 = MemoryTestUtils.generateRandomPayload( size1 );
        final Pointer allocatedPointer1 = offHeapMemoryBuffer.allocate( allocatedPayload1.length, -1, -1 );
        Assert.assertNotNull( allocatedPointer1 );
        final ByteBuffer buffer1 = allocatedPointer1.directBuffer;
        Assert.assertNotNull( buffer1 );
        Assert.assertEquals( 0, buffer1.position() );
        Assert.assertEquals( size1, buffer1.limit() );
        Assert.assertEquals( size1, buffer1.capacity() );
        buffer1.put( allocatedPayload1 );
        Assert.assertEquals( new String( allocatedPayload1 ), new String( offHeapMemoryBuffer.retrieve( allocatedPointer1 ) ) );

        final int size2 = 2 * SMALL_PAYLOAD_LENGTH;
        final byte[] allocatedPayload2 = MemoryTestUtils.generateRandomPayload( size2 );
        final Pointer allocatedPointer2 = offHeapMemoryBuffer.allocate( allocatedPayload2.length, -1, -1 );
        Assert.assertNotNull( allocatedPointer2 );
        final ByteBuffer buffer2 = allocatedPointer2.directBuffer;
        Assert.assertNotNull( buffer2 );
        Assert.assertEquals( size2, buffer2.limit() );
        Assert.assertEquals( size2, buffer2.capacity() );
        buffer2.put( allocatedPayload2 );
        Assert.assertEquals( new String( allocatedPayload2 ), new String( offHeapMemoryBuffer.retrieve( allocatedPointer2 ) ) );

        
        // Ensure the new allocation has not overwritten other data
        Assert.assertEquals( new String( payload2 ), new String( offHeapMemoryBuffer.retrieve( pointer2 ) ) );
        Assert.assertEquals( new String( payload3 ), new String( offHeapMemoryBuffer.retrieve( pointer3 ) ) );

    }
    
}
