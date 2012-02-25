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

import java.util.ArrayList;
import java.util.List;

import org.apache.directmemory.measures.Ram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemoryManagerServiceImpl<V>
    implements MemoryManagerService<V>
{

    protected static Logger logger = LoggerFactory.getLogger( MemoryManager.class );

    protected List<OffHeapMemoryBuffer<V>> buffers = new ArrayList<OffHeapMemoryBuffer<V>>();

    protected int activeBufferIndex = 0;

    public MemoryManagerServiceImpl()
    {
    }

    public void init( int numberOfBuffers, int size )
    {
        buffers = new ArrayList<OffHeapMemoryBuffer<V>>( numberOfBuffers );

        for ( int i = 0; i < numberOfBuffers; i++ )
        {
            final OffHeapMemoryBuffer<V> offHeapMemoryBuffer = instanciateOffHeapMemoryBuffer( size, i );
            buffers.add( offHeapMemoryBuffer );
        }

        logger.info( format( "MemoryManager initialized - %d buffers, %s each", numberOfBuffers, Ram.inMb( size ) ) );
    }

    protected OffHeapMemoryBuffer<V> instanciateOffHeapMemoryBuffer( int size, int bufferNumber )
    {
        return OffHeapMemoryBufferImpl.createNew( size, bufferNumber );
    }

    public OffHeapMemoryBuffer<V> getActiveBuffer()
    {
        return buffers.get( activeBufferIndex );
    }

    public Pointer<V> store( byte[] payload, int expiresIn )
    {
        Pointer<V> p = getActiveBuffer().store( payload, expiresIn );
        if ( p == null )
        {
            nextBuffer();
            p = getActiveBuffer().store( payload, expiresIn );
        }
        return p;
    }

    public Pointer<V> store( byte[] payload )
    {
        return store( payload, 0 );
    }

    public Pointer<V> update( Pointer<V> pointer, byte[] payload )
    {
        return buffers.get( pointer.getBufferNumber() ).update( pointer, payload );
    }

    public byte[] retrieve( Pointer<V> pointer )
    {
        return buffers.get( pointer.getBufferNumber() ).retrieve( pointer );
    }

    public void free( Pointer<V> pointer )
    {
        buffers.get( pointer.getBufferNumber() ).free( pointer );
    }

    public void clear()
    {
        for ( OffHeapMemoryBuffer<V> buffer : buffers )
        {
            buffer.clear();
        }
        activeBufferIndex = 0;
    }

    public long capacity()
    {
        long totalCapacity = 0;
        for ( OffHeapMemoryBuffer<V> buffer : buffers )
        {
            totalCapacity += buffer.capacity();
        }
        return totalCapacity;
    }

    public long collectExpired()
    {
        long disposed = 0;
        for ( OffHeapMemoryBuffer<V> buffer : buffers )
        {
            disposed += buffer.collectExpired();
        }
        return disposed;
    }

    public void collectLFU()
    {
        for ( OffHeapMemoryBuffer<V> buf : buffers )
        {
            buf.collectLFU( -1 );
        }
    }

    public List<OffHeapMemoryBuffer<V>> getBuffers()
    {
        return buffers;
    }

    public void setBuffers( List<OffHeapMemoryBuffer<V>> buffers )
    {
        this.buffers = buffers;
    }

    public <T extends V> Pointer<V> allocate( Class<T> type, int size, long expiresIn, long expires )
    {
        Pointer<V> p = getActiveBuffer().allocate( type, size, expiresIn, expires );
        if ( p == null )
        {
            nextBuffer();
            p = getActiveBuffer().allocate( type, size, expiresIn, expires );
        }
        return p;
    }

    protected void nextBuffer()
    {
        activeBufferIndex = ( activeBufferIndex + 1 ) % buffers.size();
    }

}
