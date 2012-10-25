package org.apache.directmemory.memory.buffer;

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
import java.nio.ByteOrder;

public abstract class AbstractMemoryBuffer
    implements MemoryBuffer
{

    protected long writerIndex = 0;

    protected long readerIndex = 0;

    @Override
    public boolean readable()
    {
        return readerIndex() < writerIndex();
    }

    @Override
    public long readableBytes()
    {
        return writerIndex() - readerIndex();
    }

    @Override
    public byte readByte()
    {
        return readByte( readerIndex++ );
    }

    @Override
    public int readBytes( byte[] bytes )
    {
        return readBytes( bytes, 0, bytes.length );
    }

    @Override
    public int readBytes( byte[] bytes, int offset, int length )
    {
        for ( int pos = offset; pos < offset + length; pos++ )
        {
            bytes[pos] = readByte();
        }
        return length;
    }

    @Override
    public int readBuffer( ByteBuffer byteBuffer )
    {
        int remaining = Math.min( byteBuffer.remaining(), (int) readableBytes() );
        return readBuffer( byteBuffer, byteBuffer.position(), remaining );
    }

    @Override
    public int readBuffer( ByteBuffer byteBuffer, int offset, int length )
    {
        if ( byteBuffer.hasArray() )
        {
            readBytes( byteBuffer.array(), offset, length );
        }
        else
        {
            for ( int pos = offset; pos < offset + length; pos++ )
            {
                byteBuffer.put( offset, readByte() );
            }
        }
        return length;
    }

    @Override
    public long readBuffer( WritableMemoryBuffer memoryBuffer )
    {
        long remaining = Math.min( memoryBuffer.writableBytes(), readableBytes() );
        return readBuffer( memoryBuffer, memoryBuffer.writerIndex(), remaining );
    }

    @Override
    public long readBuffer( WritableMemoryBuffer memoryBuffer, long offset, long length )
    {
        if ( memoryBuffer instanceof AbstractMemoryBuffer )
        {
            AbstractMemoryBuffer mb = (AbstractMemoryBuffer) memoryBuffer;
            for ( long pos = offset; pos < offset + length; pos++ )
            {
                mb.writeByte( offset, readByte() );
            }
        }
        else
        {
            long mark = memoryBuffer.writerIndex();
            memoryBuffer.writerIndex( offset );
            for ( long pos = offset; pos < offset + length; pos++ )
            {
                memoryBuffer.writeByte( readByte() );
            }
            memoryBuffer.writerIndex( Math.max( mark, memoryBuffer.writerIndex() ) );
        }
        return length;
    }

    @Override
    public short readUnsignedByte()
    {
        return (short) ( readByte() & 0xFF );
    }

    @Override
    public short readShort()
    {
        return ByteOrderUtils.getShort( this, byteOrder() == ByteOrder.BIG_ENDIAN );
    }

    @Override
    public char readChar()
    {
        return (char) readShort();
    }

    @Override
    public int readInt()
    {
        return ByteOrderUtils.getInt( this, byteOrder() == ByteOrder.BIG_ENDIAN );
    }

    @Override
    public long readLong()
    {
        return ByteOrderUtils.getLong( this, byteOrder() == ByteOrder.BIG_ENDIAN );
    }

    @Override
    public float readFloat()
    {
        return Float.intBitsToFloat( readInt() );
    }

    @Override
    public double readDouble()
    {
        return Double.longBitsToDouble( readLong() );
    }

    @Override
    public long readerIndex()
    {
        return readerIndex;
    }

    @Override
    public void readerIndex( long readerIndex )
    {
        this.readerIndex = readerIndex;
    }

    @Override
    public boolean writable()
    {
        return growing() || writerIndex() < maxCapacity();
    }

    @Override
    public void writeByte( byte value )
    {
        writeByte( writerIndex++, value );
    }

    @Override
    public long writableBytes()
    {
        return maxCapacity() - writerIndex();
    }

    @Override
    public void writeBytes( byte[] bytes )
    {
        writeBytes( bytes, 0, bytes.length );
    }

    @Override
    public void writeBytes( byte[] bytes, int offset, int length )
    {
        for ( int pos = offset; pos < length; pos++ )
        {
            writeByte( bytes[pos] );
        }
    }

    @Override
    public void writeBuffer( ByteBuffer byteBuffer )
    {
        int remaining = Math.min( byteBuffer.remaining(), (int) writableBytes() );
        writeBuffer( byteBuffer, byteBuffer.position(), remaining );
    }

    @Override
    public void writeBuffer( ByteBuffer byteBuffer, int offset, int length )
    {
        if ( byteBuffer.hasArray() )
        {
            writeBytes( byteBuffer.array(), offset, length );
        }
        else
        {
            for ( int pos = offset; pos < offset + length; pos++ )
            {
                writeByte( byteBuffer.get( offset ) );
            }
        }
    }

    @Override
    public void writeBuffer( ReadableMemoryBuffer memoryBuffer )
    {
        long remaining = Math.min( memoryBuffer.readableBytes(), writableBytes() );
        writeBuffer( memoryBuffer, memoryBuffer.readerIndex(), remaining );
    }

    @Override
    public void writeBuffer( ReadableMemoryBuffer memoryBuffer, long offset, long length )
    {
        if ( memoryBuffer instanceof AbstractMemoryBuffer )
        {
            AbstractMemoryBuffer mb = (AbstractMemoryBuffer) memoryBuffer;
            for ( long pos = offset; pos < offset + length; pos++ )
            {
                writeByte( mb.readByte( offset ) );
            }
        }
        else
        {
            long mark = memoryBuffer.readerIndex();
            memoryBuffer.readerIndex( offset );
            for ( long pos = offset; pos < offset + length; pos++ )
            {
                writeByte( memoryBuffer.readByte() );
            }
            memoryBuffer.readerIndex( Math.max( mark, memoryBuffer.readerIndex() ) );
        }
    }

    @Override
    public void writeUnsignedByte( short value )
    {
        writeByte( (byte) value );
    }

    @Override
    public void writeShort( short value )
    {
        ByteOrderUtils.putShort( value, this, byteOrder() == ByteOrder.BIG_ENDIAN );
    }

    @Override
    public void writeChar( char value )
    {
        writeShort( (short) value );
    }

    @Override
    public void writeInt( int value )
    {
        ByteOrderUtils.putInt( value, this, byteOrder() == ByteOrder.BIG_ENDIAN );
    }

    @Override
    public void writeLong( long value )
    {
        ByteOrderUtils.putLong( value, this, byteOrder() == ByteOrder.BIG_ENDIAN );
    }

    @Override
    public void writeFloat( float value )
    {
        writeInt( Float.floatToIntBits( value ) );
    }

    @Override
    public void writeDouble( double value )
    {
        writeLong( Double.doubleToLongBits( value ) );
    }

    @Override
    public long writerIndex()
    {
        return writerIndex;
    }

    @Override
    public void writerIndex( long writerIndex )
    {
        this.writerIndex = writerIndex;
    }

    protected void rangeCheck( long offset )
    {
        if ( offset < 0 )
        {
            throw new IndexOutOfBoundsException( String.format( "Offset %s is below 0", offset ) );
        }
        if ( offset >= maxCapacity() )
        {
            throw new IndexOutOfBoundsException( String.format( "Offset %s is higher than maximum legal index ",
                                                                offset, ( maxCapacity() - 1 ) ) );
        }
    }

    protected abstract void writeByte( long offset, byte value );

    protected abstract byte readByte( long offset );

}
