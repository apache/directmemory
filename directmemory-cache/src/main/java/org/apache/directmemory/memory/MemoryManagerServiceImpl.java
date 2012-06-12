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

import com.google.common.base.Predicate;
import org.apache.directmemory.measures.Ram;
import org.apache.directmemory.memory.allocator.ByteBufferAllocator;
import org.apache.directmemory.memory.allocator.MergingByteBufferAllocatorImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.limit;
import static com.google.common.collect.Ordering.from;
import static java.lang.String.format;

public class MemoryManagerServiceImpl<V>
    implements MemoryManagerService<V>
{

    protected static final long NEVER_EXPIRES = 0L;

    protected static Logger logger = LoggerFactory.getLogger( MemoryManager.class );

    private List<ByteBufferAllocator> allocators;

    private final Set<Pointer<V>> pointers = Collections.newSetFromMap( new ConcurrentHashMap<Pointer<V>, Boolean>() );

    private final boolean returnNullWhenFull;

    protected final AtomicLong used = new AtomicLong( 0L );

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

        allocators = new ArrayList<ByteBufferAllocator>( numberOfBuffers );

        for ( int i = 0; i < numberOfBuffers; i++ )
        {
            final ByteBufferAllocator allocator = instanciateByteBufferAllocator( i, size );
            allocators.add( allocator );
        }

        allocationPolicy.init( allocators );

        logger.info( format( "MemoryManager initialized - %d buffers, %s each", numberOfBuffers, Ram.inMb( size ) ) );
    }


    protected ByteBufferAllocator instanciateByteBufferAllocator( final int allocatorNumber, final int size )
    {
        final MergingByteBufferAllocatorImpl allocator = new MergingByteBufferAllocatorImpl( allocatorNumber, size );

        // Hack to ensure the pointers are always split to keep backward compatibility.
        allocator.setMinSizeThreshold( 0 );
        allocator.setSizeRatioThreshold( 1.0 );

        return allocator;
    }

    protected ByteBufferAllocator getAllocator( int allocatorIndex )
    {
        return allocators.get( allocatorIndex );
    }

    protected ByteBufferAllocator getCurrentAllocator()
    {
        return allocationPolicy.getActiveAllocator( null, 0 );
    }

    @Override
    public Pointer<V> store( byte[] payload, int expiresIn )
    {
        Pointer<V> p = null;
        ByteBufferAllocator allocator = null;
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
            final ByteBuffer buffer = allocator.allocate( payload.length );

            if ( buffer == null )
            {
                continue;
            }

            p = instanciatePointer( buffer, allocator.getNumber(), expiresIn, NEVER_EXPIRES );

            buffer.rewind();
            buffer.put( payload );

            used.addAndGet( payload.length );

        }
        while ( p == null );
        return p;
    }

    @Override
    public Pointer<V> store( byte[] payload )
    {
        return store( payload, 0 );
    }

    @Override
    public Pointer<V> update( Pointer<V> pointer, byte[] payload )
    {
        free( pointer );
        return store( payload );
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

        final ByteBuffer buf = pointer.getDirectBuffer().asReadOnlyBuffer();
        buf.rewind();

        final byte[] swp = new byte[buf.limit()];
        buf.get( swp );
        return swp;
    }

    @Override
    public void free( final Pointer<V> pointer )
    {
        if ( !pointers.remove( pointer ) )
        {
            // pointers has been already freed.
            //throw new IllegalArgumentException( "This pointer " + pointer + " has already been freed" );
            return;
        }

        getAllocator( pointer.getBufferNumber() ).free( pointer.getDirectBuffer() );

        used.addAndGet( -pointer.getCapacity() );

        pointer.setFree( true );
    }

    @Override
    public void clear()
    {
        for ( Pointer<V> pointer : pointers )
        {
            pointer.setFree( true );
        }
        pointers.clear();
        for ( ByteBufferAllocator allocator : allocators )
        {
            allocator.clear();
        }
        allocationPolicy.reset();
    }

    @Override
    public long capacity()
    {
        long totalCapacity = 0;
        for ( ByteBufferAllocator allocator : allocators )
        {
            totalCapacity += allocator.getCapacity();
        }
        return totalCapacity;
    }

    @Override
    public long used()
    {
        return used.get();
    }

    private final Predicate<Pointer<V>> relative = new Predicate<Pointer<V>>()
    {

        @Override
        public boolean apply( Pointer<V> input )
        {
            return !input.isFree() && input.isExpired();
        }

    };

    private final Predicate<Pointer<V>> absolute = new Predicate<Pointer<V>>()
    {

        @Override
        public boolean apply( Pointer<V> input )
        {
            return !input.isFree() && input.isExpired();
        }

    };

    @Override
    public long collectExpired()
    {
        int limit = 50;
        return free( limit( filter( pointers, relative ), limit ) ) + free(
            limit( filter( pointers, absolute ), limit ) );

    }

    @Override
    public void collectLFU()
    {

        int limit = pointers.size() / 10;

        Iterable<Pointer<V>> result = from( new Comparator<Pointer<V>>()
        {

            public int compare( Pointer<V> o1, Pointer<V> o2 )
            {
                float f1 = o1.getFrequency();
                float f2 = o2.getFrequency();

                return Float.compare( f1, f2 );
            }

        } ).sortedCopy( limit( filter( pointers, new Predicate<Pointer<V>>()
        {

            @Override
            public boolean apply( Pointer<V> input )
            {
                return !input.isFree();
            }

        } ), limit ) );

        free( result );

    }

    protected long free( Iterable<Pointer<V>> pointers )
    {
        long howMuch = 0;
        for ( Pointer<V> expired : pointers )
        {
            howMuch += expired.getCapacity();
            free( expired );
        }
        return howMuch;
    }


    protected List<ByteBufferAllocator> getAllocators()
    {
        return allocators;
    }

    @Deprecated
    @Override
    public <T extends V> Pointer<V> allocate( final Class<T> type, final int size, final long expiresIn,
                                              final long expires )
    {

        Pointer<V> p = null;
        ByteBufferAllocator allocator = null;
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

            final ByteBuffer buffer = allocator.allocate( size );

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

    protected Pointer<V> instanciatePointer( final ByteBuffer buffer, final int allocatorIndex, final long expiresIn,
                                             final long expires )
    {

        Pointer<V> p = new PointerImpl<V>();

        p.setDirectBuffer( buffer );
        p.setExpiration( expires, expiresIn );
        p.setBufferNumber( allocatorIndex );
        p.setFree( false );
        p.createdNow();

        pointers.add( p );

        return p;
    }

    protected boolean returnsNullWhenFull()
    {
        return returnNullWhenFull;
    }

    public Set<Pointer<V>> getPointers()
    {
        return Collections.unmodifiableSet( pointers );
    }
}
