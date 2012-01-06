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
import org.apache.directmemory.measures.Ram;
import org.apache.directmemory.misc.Format;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemoryManagerServiceImpl
    implements MemoryManagerService
{

    protected static Logger logger = LoggerFactory.getLogger( MemoryManager.class );

    private List<OffHeapMemoryBuffer> buffers = new ArrayList<OffHeapMemoryBuffer>();

    private int activeBufferIndex = 0;

    public MemoryManagerServiceImpl()
    {
    }

    public void init( int numberOfBuffers, int size )
    {
        buffers = new ArrayList<OffHeapMemoryBuffer>( numberOfBuffers );

        for ( int i = 0; i < numberOfBuffers; i++ )
        {
            final OffHeapMemoryBuffer offHeapMemoryBuffer = instanciateOffHeapMemoryBuffer( size, i );
            buffers.add( offHeapMemoryBuffer );
        }

        logger.info( Format.it( "MemoryManager initialized - %d buffers, %s each", numberOfBuffers, Ram.inMb( size ) ) );
    }

    protected OffHeapMemoryBuffer instanciateOffHeapMemoryBuffer( int size, int bufferNumber )
    {
        return OffHeapMemoryBufferImpl.createNew( size, bufferNumber );
    }

    public OffHeapMemoryBuffer getActiveBuffer()
    {
        return buffers.get( activeBufferIndex );
    }

    public Pointer store( byte[] payload, int expiresIn )
    {
        Pointer p = getActiveBuffer().store( payload, expiresIn );
        if ( p == null )
        {
            nextBuffer();
            p = getActiveBuffer().store( payload, expiresIn );
        }
        return p;
    }

    public Pointer store( byte[] payload )
    {
        return store( payload, 0 );
    }

    public Pointer update( Pointer pointer, byte[] payload )
    {
        return buffers.get( pointer.bufferNumber ).update( pointer, payload );
    }

    public byte[] retrieve( Pointer pointer )
    {
        return buffers.get( pointer.bufferNumber ).retrieve( pointer );
    }

    public void free( Pointer pointer )
    {
        buffers.get( pointer.bufferNumber ).free( pointer );
    }

    public void clear()
    {
        for ( OffHeapMemoryBuffer buffer : buffers )
        {
            buffer.clear();
        }
        activeBufferIndex = 0;
    }

    public long capacity()
    {
        long totalCapacity = 0;
        for ( OffHeapMemoryBuffer buffer : buffers )
        {
            totalCapacity += buffer.capacity();
        }
        return totalCapacity;
    }

    public long collectExpired()
    {
        long disposed = 0;
        for ( OffHeapMemoryBuffer buffer : buffers )
        {
            disposed += buffer.collectExpired();
        }
        return disposed;
    }

    public void collectLFU()
    {
        for ( OffHeapMemoryBuffer buf : buffers )
        {
            buf.collectLFU( -1 );
        }
    }

    public List<OffHeapMemoryBuffer> getBuffers()
    {
        return buffers;
    }

    public void setBuffers( List<OffHeapMemoryBuffer> buffers )
    {
        this.buffers = buffers;
    }

    @Override
    public Pointer allocate( int size, long expiresIn, long expires )
    {
        Pointer p = getActiveBuffer().allocate( size, expiresIn, expires );
        if ( p == null )
        {
            nextBuffer();
            p = getActiveBuffer().allocate( size, expiresIn, expires );
        }
        return p;
    }

    protected void nextBuffer()
    {
        activeBufferIndex = ( activeBufferIndex + 1 ) % buffers.size();
    }

}
