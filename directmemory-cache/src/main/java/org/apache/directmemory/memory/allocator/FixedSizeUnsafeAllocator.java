package org.apache.directmemory.memory.allocator;

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
import java.nio.BufferOverflowException;
import java.nio.ByteOrder;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.directmemory.memory.IllegalMemoryPointerException;
import org.apache.directmemory.memory.buffer.AbstractMemoryBuffer;
import org.apache.directmemory.memory.buffer.MemoryBuffer;

public class FixedSizeUnsafeAllocator
    implements Allocator
{

    private final sun.misc.Unsafe unsafe = UnsafeUtils.getUnsafe();

    private final Queue<UnsafeMemoryBuffer> memoryBuffers = new ConcurrentLinkedQueue<UnsafeMemoryBuffer>();

    private final int number;

    private final int size;

    // Tells if it returns null or throw an BufferOverflowException when the requested size is bigger than the size of
    // the slices
    private final boolean returnNullWhenOversizingSliceSize = true;

    public FixedSizeUnsafeAllocator( int number, int size )
    {
        this.number = number;
        this.size = size;

        if ( unsafe == null )
        {
            throw new IllegalStateException( "This JVM has no sun.misc.Unsafe support, "
                + "please choose another MemoryManager implementation" );
        }

        for ( int i = 0; i < number; i++ )
        {
            long baseAddress = unsafe.allocateMemory( size );
            UnsafeMemoryBuffer memoryBuffer = new UnsafeMemoryBuffer( baseAddress, size );
            memoryBuffers.add( memoryBuffer );
        }
    }

    @Override
    public void close()
        throws IOException
    {
        clear();
        Iterator<UnsafeMemoryBuffer> iterator = memoryBuffers.iterator();
        while ( iterator.hasNext() )
        {
            UnsafeMemoryBuffer memoryBuffer = iterator.next();
            memoryBuffer.free();
            iterator.remove();
        }
    }

    @Override
    public void free( MemoryBuffer memoryBuffer )
    {
        memoryBuffer.clear();
        memoryBuffers.offer( (UnsafeMemoryBuffer) memoryBuffer );
    }

    @Override
    public MemoryBuffer allocate( int size )
    {
        return findFreeBuffer( size );
    }

    @Override
    public void clear()
    {
        for (UnsafeMemoryBuffer memoryBuffer : memoryBuffers) {
            unsafe.setMemory(memoryBuffer.baseAddress, memoryBuffer.capacity, (byte) 0);
        }
    }

    @Override
    public int getCapacity()
    {
        long capacity = 0;
        for (UnsafeMemoryBuffer memoryBuffer : memoryBuffers) {
            capacity += memoryBuffer.capacity;
        }
        return (int) capacity;
    }

    @Override
    public int getNumber()
    {
        return number;
    }

    protected MemoryBuffer findFreeBuffer( int capacity )
    {
        // ensure the requested size is not bigger than the slices' size
        if ( capacity > size )
        {
            if ( returnNullWhenOversizingSliceSize )
            {
                return null;
            }
            else
            {
                throw new BufferOverflowException();
            }
        }
        // TODO : Add capacity to wait till a given timeout for a freed buffer
        return memoryBuffers.poll();
    }

    private class UnsafeMemoryBuffer
        extends AbstractMemoryBuffer
    {

        private final long baseAddress;

        private final long capacity;

        private ByteOrder byteOrder = ByteOrder.BIG_ENDIAN;

        private UnsafeMemoryBuffer( long baseAddress, long capacity )
        {
            if ( baseAddress == 0 )
            {
                throw new IllegalMemoryPointerException( "The pointers base address is not legal" );
            }

            this.baseAddress = baseAddress;
            this.capacity = capacity;
        }

        @Override
        public long capacity()
        {
            return writerIndex();
        }

        @Override
        public long maxCapacity()
        {
            return capacity;
        }

        @Override
        public boolean growing()
        {
            return false;
        }

        @Override
        public ByteOrder byteOrder()
        {
            return byteOrder;
        }

        @Override
        public void byteOrder( ByteOrder byteOrder )
        {
            this.byteOrder = byteOrder;
        }

        @Override
        public void free()
        {
            unsafe.freeMemory( baseAddress );
        }

        @Override
        public void clear()
        {
            unsafe.setMemory( baseAddress, capacity, (byte) 0 );
            writerIndex = 0;
            readerIndex = 0;
        }

        @Override
        protected void writeByte( long offset, byte value )
        {
            unsafe.putByte( baseAddress + offset, value );
        }

        @Override
        protected byte readByte( long offset )
        {
            return unsafe.getByte( baseAddress + offset );
        }

        @Override
        public short readShort()
        {
            short value = unsafe.getShort( baseAddress + readerIndex );
            readerIndex += 2;
            return value;
        }

        @Override
        public char readChar()
        {
            char value = unsafe.getChar( baseAddress + readerIndex );
            readerIndex += 2;
            return value;
        }

        @Override
        public int readInt()
        {
            int value = unsafe.getInt( baseAddress + readerIndex );
            readerIndex += 4;
            return value;
        }

        @Override
        public long readLong()
        {
            long value = unsafe.getLong( baseAddress + readerIndex );
            readerIndex += 8;
            return value;
        }

        @Override
        public float readFloat()
        {
            float value = unsafe.getFloat( baseAddress + readerIndex );
            readerIndex += 4;
            return value;
        }

        @Override
        public double readDouble()
        {
            double value = unsafe.getDouble( baseAddress + readerIndex );
            readerIndex += 8;
            return value;
        }

        @Override
        public void writeShort( short value )
        {
            unsafe.putShort( baseAddress + writerIndex, value );
            writerIndex += 2;
        }

        @Override
        public void writeChar( char value )
        {
            unsafe.putChar( baseAddress + writerIndex, value );
            writerIndex += 2;
        }

        @Override
        public void writeInt( int value )
        {
            unsafe.putInt( baseAddress + writerIndex, value );
            writerIndex += 4;
        }

        @Override
        public void writeLong( long value )
        {
            unsafe.putLong( baseAddress + writerIndex, value );
            writerIndex += 8;
        }

        @Override
        public void writeFloat( float value )
        {
            unsafe.putFloat( baseAddress + writerIndex, value );
            writerIndex += 4;
        }

        @Override
        public void writeDouble( double value )
        {
            unsafe.putDouble( baseAddress + writerIndex, value );
            writerIndex += 8;
        }

    }

}
