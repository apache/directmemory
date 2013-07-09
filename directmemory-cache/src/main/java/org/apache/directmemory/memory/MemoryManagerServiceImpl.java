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

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.directmemory.measures.Ram;
import org.apache.directmemory.memory.allocator.Allocator;
import org.apache.directmemory.memory.allocator.MergingByteBufferAllocator;
import org.apache.directmemory.memory.buffer.MemoryBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemoryManagerServiceImpl<V>
    extends AbstractMemoryManager<V>
    implements MemoryManagerService<V>
{

    protected static final Logger logger = LoggerFactory.getLogger( MemoryManager.class );

    List<Allocator> allocators;

    protected final AllocationPolicy allocationPolicy;

    public MemoryManagerServiceImpl()
    {
        this( true );
    }

    public MemoryManagerServiceImpl( final boolean returnNullWhenFull )
    {
        this( new RoundRobinAllocationPolicy(), returnNullWhenFull );
    }

    public MemoryManagerServiceImpl( final AllocationPolicy allocationPolicy, final boolean returnNullWhenFull )
    {
        this.allocationPolicy = allocationPolicy;
        this.returnNullWhenFull = returnNullWhenFull;
    }

    @Override
    public void init( int numberOfBuffers, int size )
    {

        allocators = new ArrayList<Allocator>( numberOfBuffers );

        for ( int i = 0; i < numberOfBuffers; i++ )
        {
            final Allocator allocator = instanciateByteBufferAllocator( i, size );
            allocators.add( allocator );
        }

        allocationPolicy.init( allocators );

        logger.info( format( "MemoryManager initialized - %d buffers, %s each", numberOfBuffers, Ram.inMb( size ) ) );
    }

    @Override
    public void close()
        throws IOException
    {
        Iterator<Allocator> iterator = allocators.iterator();
        while ( iterator.hasNext() )
        {
            Allocator allocator = iterator.next();
            allocator.close();
            iterator.remove();
        }
        used.set( 0 );
    }

    protected Allocator instanciateByteBufferAllocator( final int allocatorNumber, final int size )
    {
        final MergingByteBufferAllocator allocator = new MergingByteBufferAllocator( allocatorNumber, size );

        // Hack to ensure the pointers are always split to keep backward compatibility.
        allocator.setMinSizeThreshold( 0 );
        allocator.setSizeRatioThreshold( 1.0 );

        return allocator;
    }

    protected Allocator getAllocator( int allocatorIndex )
    {
        return allocators.get( allocatorIndex );
    }

    protected Allocator getCurrentAllocator()
    {
        return allocationPolicy.getActiveAllocator( null, 0 );
    }

    @Override
    public Pointer<V> store( byte[] payload, long expiresIn )
    {
        Pointer<V> p = null;
        Allocator allocator = null;
        int allocationNumber = 0;
        do
        {
            allocationNumber++;
            allocator = allocationPolicy.getActiveAllocator( allocator, allocationNumber );
            if ( allocator == null )
            {
                if ( returnsNullWhenFull() )
                {
                    return null;
                }
                else
                {
                    throw new BufferOverflowException();
                }
            }
            final MemoryBuffer buffer = allocator.allocate( payload.length );

            if ( buffer == null )
            {
                continue;
            }

            p = instanciatePointer( buffer, allocator.getNumber(), expiresIn, NEVER_EXPIRES );

            buffer.writerIndex( 0 );
            buffer.writeBytes( payload );

            used.addAndGet( payload.length );

        }
        while ( p == null );
        return p;
    }

    @Override
    public byte[] retrieve( final Pointer<V> pointer )
    {
        // check if pointer has not been freed before
        if ( !pointers.contains( pointer ) )
        {
            return null;
        }

        pointer.hit();

        final MemoryBuffer buf = pointer.getMemoryBuffer();
        buf.readerIndex( 0 );

        final byte[] swp = new byte[(int) buf.readableBytes()];
        buf.readBytes( swp );
        return swp;
    }

    @Override
    public Pointer<V> free( final Pointer<V> pointer )
    {
        if ( !pointers.remove( pointer ) )
        {
            // pointers has been already freed.
            // throw new IllegalArgumentException( "This pointer " + pointer + " has already been freed" );
            return pointer;
        }

        getAllocator( pointer.getBufferNumber() ).free( pointer.getMemoryBuffer() );

        used.addAndGet( -pointer.getCapacity() );

        pointer.setFree( true );

        return pointer;
    }

    @Override
    public long capacity()
    {
        long totalCapacity = 0;
        for ( Allocator allocator : allocators )
        {
            totalCapacity += allocator.getCapacity();
        }
        return totalCapacity;
    }

    protected List<Allocator> getAllocators()
    {
        return allocators;
    }

    @Deprecated
    @Override
    public <T extends V> Pointer<V> allocate( final Class<T> type, final int size, final long expiresIn,
                                              final long expires )
    {

        Pointer<V> p = null;
        Allocator allocator = null;
        int allocationNumber = 0;
        do
        {
            allocationNumber++;
            allocator = allocationPolicy.getActiveAllocator( allocator, allocationNumber );
            if ( allocator == null )
            {
                if ( returnsNullWhenFull() )
                {
                    return null;
                }
                else
                {
                    throw new BufferOverflowException();
                }
            }

            final MemoryBuffer buffer = allocator.allocate( size );

            if ( buffer == null )
            {
                continue;
            }

            p = instanciatePointer( buffer, allocator.getNumber(), expiresIn, NEVER_EXPIRES );

            used.addAndGet( size );
        }
        while ( p == null );

        p.setClazz( type );

        return p;
    }

    @Override
    public void clear()
    {
        for ( Pointer<V> pointer : pointers )
        {
            pointer.setFree( true );
        }
        pointers.clear();
        for ( Allocator allocator : allocators )
        {
            allocator.clear();
        }
        allocationPolicy.reset();
        used.set(0L);
    }

    protected Pointer<V> instanciatePointer( final MemoryBuffer buffer, final int allocatorIndex, final long expiresIn,
                                             final long expires )
    {

        Pointer<V> p = new PointerImpl<V>( buffer, allocatorIndex );

        p.setExpiration( expires, expiresIn );
        p.setFree( false );
        p.createdNow();

        pointers.add( p );

        return p;
    }
}
