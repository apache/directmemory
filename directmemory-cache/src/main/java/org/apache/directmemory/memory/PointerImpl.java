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

import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.directmemory.memory.buffer.MemoryBuffer;

public class PointerImpl<T>
    implements Pointer<T>
{

    public final MemoryBuffer memoryBuffer;

    public final int bufferNumber;

    public long created;

    public long expires;

    public long expiresIn;

    public long hits;

    public AtomicBoolean free = new AtomicBoolean( true );

    public AtomicLong lastHit = new AtomicLong();

    public Class<? extends T> clazz;

    public PointerImpl( MemoryBuffer memoryBuffer, int bufferNumber )
    {
        this.memoryBuffer = memoryBuffer;
        this.bufferNumber = bufferNumber;
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
        return memoryBuffer == null ? -1 : memoryBuffer.capacity();
    }

    @Override
    public String toString()
    {
        return format( "%s[%s] %s free", getClass().getSimpleName(), getSize(), ( isFree() ? "" : "not" ) );
    }

    @Override
    public void reset()
    {
        free.set( true );
        created = 0;
        lastHit.set( 0 );
        hits = 0;
        expiresIn = 0;
        clazz = null;
        memoryBuffer.clear();
    }

    @Override
    public boolean isFree()
    {
        return free.get();
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
    public long getSize()
    {
        return memoryBuffer.capacity();
    }

    @Override
    public void hit()
    {
        lastHit.set( System.currentTimeMillis() );
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
        this.free.set( free );
    }

    @Override
    public void setClazz( Class<? extends T> clazz )
    {
        this.clazz = clazz;
    }

    @Override
    public void createdNow()
    {
        created = System.currentTimeMillis();
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
