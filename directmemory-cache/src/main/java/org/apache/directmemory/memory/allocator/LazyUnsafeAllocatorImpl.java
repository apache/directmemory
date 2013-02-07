package org.apache.directmemory.memory.allocator;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteOrder;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.directmemory.memory.IllegalMemoryPointerException;
import org.apache.directmemory.memory.buffer.AbstractMemoryBuffer;
import org.apache.directmemory.memory.buffer.MemoryBuffer;

public class LazyUnsafeAllocatorImpl
    implements Allocator
{

    private final sun.misc.Unsafe unsafe = UnsafeUtils.getUnsafe();

    private final Set<UnsafeMemoryBuffer> memoryBuffers =
        Collections.newSetFromMap( new ConcurrentHashMap<UnsafeMemoryBuffer, Boolean>() );

    private final AtomicLong used = new AtomicLong( 0 );

    private final int number;

    private final long capacity;

    // Tells if it returns null or throw an BufferOverflowException when the requested size is bigger than the size of
    // the slices
    private final boolean returnNullWhenOversizingSliceSize = true;

    public LazyUnsafeAllocatorImpl( int number, long capacity )
    {
        this.number = number;
        this.capacity = capacity;

        if ( unsafe == null )
        {
            throw new IllegalStateException( "This JVM has no sun.misc.Unsafe support, "
                + "please choose another MemoryManager implementation" );
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
        used.addAndGet( -memoryBuffer.capacity() );
        memoryBuffer.free();
        memoryBuffers.remove( memoryBuffer );
    }

    @Override
    public MemoryBuffer allocate( int size )
    {
        if ( capacity < used.get() + size )
        {
            throw new BufferOverflowException();
        }

        long baseAddress = unsafe.allocateMemory( size );
        UnsafeMemoryBuffer memoryBuffer = new UnsafeMemoryBuffer( baseAddress, size );
        memoryBuffers.add( memoryBuffer );
        used.addAndGet( size );
        return memoryBuffer;
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

    private class UnsafeMemoryBuffer
        extends AbstractMemoryBuffer
    {

        private final long baseAddress;

        private final long capacity;

        private ByteOrder byteOrder = ByteOrder.BIG_ENDIAN;

        private UnsafeMemoryBuffer( long baseAddress, long capacity )
        {
            if ( capacity > 0 && baseAddress == 0 )
            {
                throw new IllegalMemoryPointerException( "The pointers base address is not legal" );
            }

            this.baseAddress = baseAddress;
            this.capacity = capacity;
        }

        @Override
        public long capacity()
        {
            return capacity;
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
