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

import org.apache.directmemory.memory.buffer.AbstractMemoryBuffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

abstract class NioMemoryBuffer
    extends AbstractMemoryBuffer
{

    private final ByteBuffer byteBuffer;

    NioMemoryBuffer( ByteBuffer byteBuffer )
    {
        this.byteBuffer = byteBuffer;
    }

    @Override
    public long capacity()
    {
        return byteBuffer.limit();
    }

    @Override
    public long maxCapacity()
    {
        return byteBuffer.capacity();
    }

    @Override
    public ByteOrder byteOrder()
    {
        return byteBuffer.order();
    }

    @Override
    public void byteOrder( ByteOrder byteOrder )
    {
        byteBuffer.order( byteOrder );
    }

    @Override
    public void clear()
    {
        byteBuffer.clear();
        byteBuffer.rewind();
        writerIndex = 0;
        readerIndex = 0;
    }

    @Override
    public boolean readable()
    {
        return byteBuffer.remaining() > 0;
    }

    @Override
    public void readerIndex( long readerIndex )
    {
        super.readerIndex( readerIndex );
        byteBuffer.position( (int) readerIndex );
    }

    @Override
    public void writerIndex( long writerIndex )
    {
        super.writerIndex( writerIndex );
        byteBuffer.position( (int) writerIndex );
    }

    @Override
    public int readBytes( byte[] bytes, int offset, int length )
    {
        byteBuffer.get( bytes, offset, length );
        readerIndex += length;
        return length;
    }

    @Override
    protected byte readByte( long offset )
    {
        return byteBuffer.get( (int) offset );
    }

    @Override
    public boolean writable()
    {
        return byteBuffer.position() < byteBuffer.capacity();
    }

    @Override
    protected void writeByte( long offset, byte value )
    {
        byteBuffer.put( (int) offset, value );
    }

    @Override
    public void writeBytes( byte[] bytes, int offset, int length )
    {
        byteBuffer.put( bytes, offset, length );
        writerIndex += length;
    }

    protected ByteBuffer getByteBuffer() {
        return byteBuffer;
    }

}
