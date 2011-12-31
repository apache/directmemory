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
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import org.apache.directmemory.measures.Ram;
import org.apache.directmemory.misc.Format;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Vector;

public class MemoryManagerServiceImpl
    implements MemoryManagerService
{

    private static final Logger LOG = LoggerFactory.getLogger( MemoryManager.class );

    private List<OffHeapMemoryBuffer> buffers = new Vector<OffHeapMemoryBuffer>();

    private OffHeapMemoryBuffer activeBuffer;

    public MemoryManagerServiceImpl()
    {
    }

    public void init( int numberOfBuffers, int size )
    {
        for ( int i = 0; i < numberOfBuffers; i++ )
        {
            buffers.add( OffHeapMemoryBuffer.createNew( size, i ) );
        }
        activeBuffer = buffers.get( 0 );
        LOG.info( Format.it( "MemoryManager initialized - %d buffers, %s each", numberOfBuffers, Ram.inMb( size ) ) );
    }

    public Pointer store( byte[] payload, int expiresIn )
    {
        Pointer p = activeBuffer.store( payload, expiresIn );
        if ( p == null )
        {
            if ( activeBuffer.bufferNumber + 1 == buffers.size() )
            {
                return null;
            }
            else
            {
                // try next buffer
                activeBuffer = buffers.get( activeBuffer.bufferNumber + 1 );
                p = activeBuffer.store( payload, expiresIn );
            }
        }
        return p;
    }

    public Pointer store( byte[] payload )
    {
        return store( payload, 0 );
    }

    public Pointer update( Pointer pointer, byte[] payload )
    {
        Pointer p = activeBuffer.update( pointer, payload );
        if ( p == null )
        {
            if ( activeBuffer.bufferNumber == buffers.size() )
            {
                return null;
            }
            else
            {
                // try next buffer
                activeBuffer = buffers.get( activeBuffer.bufferNumber + 1 );
                p = activeBuffer.store( payload );
            }
        }
        return p;
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
        activeBuffer = buffers.get( 0 );
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

    public OffHeapMemoryBuffer getActiveBuffer()
    {
        return activeBuffer;
    }

    public void setActiveBuffer( OffHeapMemoryBuffer activeBuffer )
    {
        this.activeBuffer = activeBuffer;
    }

    @Override
    public Pointer allocate( int size, int expiresIn, int expires )
    {
        Pointer p = activeBuffer.allocate( size, expiresIn, expires );
        if ( p == null )
        {
            if ( activeBuffer.bufferNumber + 1 == buffers.size() )
            {
                return null;
            }
            else
            {
                // try next buffer
                activeBuffer = buffers.get( activeBuffer.bufferNumber + 1 );
                p = activeBuffer.allocate( size, expiresIn, expires );
            }
        }
        return p;
    }
}
