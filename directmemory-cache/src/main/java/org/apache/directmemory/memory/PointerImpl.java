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

import org.apache.directmemory.memory.buffer.MemoryBuffer;

import static java.lang.System.currentTimeMillis;
import static java.lang.String.format;

import java.nio.ByteBuffer;

public class PointerImpl<T>
    implements Pointer<T>
{
    public long start;

    public long size;

    public long created;

    public long expires;

    public long expiresIn;

    public long hits;

    public boolean free;

    public long lastHit;

    public int bufferNumber;

    public Class<? extends T> clazz;

    public MemoryBuffer memoryBuffer = null;

    public PointerImpl()
    {
    }

    public PointerImpl( long start, long end )
    {
        this.start = start;
        this.size = end;
    }

    @Override
    public byte[] content()
    {
        return null;
    }

    @Override
    public float getFrequency()
    {
        return (float) ( currentTimeMillis() - created ) / hits;
    }

    @Override
    public long getCapacity()
    {
        if (memoryBuffer != null)
            return memoryBuffer == null ? size - start + 1 : memoryBuffer.capacity();
        else
            return size;
    }

    @Override
    public String toString()
    {
        return format( "%s[%s, %s] %s free", getClass().getSimpleName(), start, size, ( free ? "" : "not" ) );
    }

    @Override
    public void reset()
    {
        free = true;
        created = 0;
        lastHit = 0;
        hits = 0;
        expiresIn = 0;
        clazz = null;
        memoryBuffer = null;
    }

    @Override
    public boolean isFree()
    {
        return free;
    }

    @Override
    public boolean isExpired()
    {
        if ( expires > 0 || expiresIn > 0 )
        {
            return ( expiresIn + created < currentTimeMillis() );
        }
        return false;
    }

    @Override
    public int getBufferNumber()
    {
        return bufferNumber;
    }

    @Override
    public long getStart()
    {
        return start;
    }

    @Override
    public long getSize()
    {
        return size;
    }

    @Override
    public void setStart( long start )
    {
        this.start = start;
    }

    @Override
    public void hit()
    {
        lastHit = System.currentTimeMillis();
        hits++;
    }

    @Override
    public Class<? extends T> getClazz()
    {
        return clazz;
    }

    @Override
    public MemoryBuffer getMemoryBuffer()
    {
        return memoryBuffer;
    }

    @Override
    public void setFree( boolean free )
    {
        this.free = free;
    }

    @Override
    public void setEnd( long end )
    {
        this.size = end;
    }

    @Override
    public void setClazz( Class<? extends T> clazz )
    {
        this.clazz = clazz;
    }

    @Override
    public void setMemoryBuffer(MemoryBuffer memoryBuffer)
    {
        this.memoryBuffer = memoryBuffer;
        this.start = 0;
        this.size = memoryBuffer.capacity();
    }

    @Override
    public void createdNow()
    {
        created = System.currentTimeMillis();
    }

    @Override
    public void setBufferNumber( int bufferNumber )
    {
        this.bufferNumber = bufferNumber;
    }

    @Override
    public void setExpiration( long expires, long expiresIn )
    {
        this.expires = expires;
        this.expiresIn = expiresIn;
    }

    @Override
    public long getExpires()
    {
        return this.expires;
    }

    @Override
    public long getExpiresIn()
    {
        return this.expiresIn;
    }
}
