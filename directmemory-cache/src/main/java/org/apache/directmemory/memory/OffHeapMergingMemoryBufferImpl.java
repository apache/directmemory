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

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.directmemory.measures.Ram;
import org.apache.directmemory.misc.Format;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link OffHeapMemoryBuffer} implementation that internally user 3 data structures to
 * store the pointers :
 * 
 * - 1 sorted list backed by a ({@link TreeMap}) that store the free pointers sorted
 * by size desc, used to allocate memory efficiently : if the first pointer has
 * not enough capacity, then no other pointers will.
 * 
 * - 1 sorted list backed by a ({@link TreeMap}) that store the free pointers sorted
 * by address offset, used to merge freed pointer efficiently : when freeing a
 * pointer, direct lookup and navigation in this list will find adjacent
 * pointers
 * 
 * - 1 set backed by ({@link ConcurrentHashMap}) of used pointers, to not loose a
 * reference to a pointer, and to be able to the buffer and stay in a consistent
 * state
 * 
 * {@link TreeMap} can be safely used because synchronization is achieved through a {@link Lock}
 * 
 * @author bperroud
 * 
 */
public class OffHeapMergingMemoryBufferImpl
    extends AbstractOffHeapMemoryBuffer
{

    protected static Logger logger = LoggerFactory.getLogger( OffHeapMemoryBufferImpl.class );

    // Default value as most list and set are backed by maps.
    private final static Boolean DEFAULT_VALUE = Boolean.TRUE;

    // List of free pointers sorted by size
    private final TreeMap<Pointer, Boolean> freePointersBySizeDesc = new TreeMap<Pointer, Boolean>(
                                                                                                    new PointerBySizeDesc() );

    // List of free pointers sorted by memory offset
    private final TreeMap<Pointer, Boolean> freePointersByMemoryOffsetAsc = new TreeMap<Pointer, Boolean>(
                                                                                                           new PointerByMemoryOffsetAsc() );

    // Set of used pointers
    private final Set<Pointer> usedPointers = Collections.newSetFromMap( new ConcurrentHashMap<Pointer, Boolean>() );

    // Lock used instead of synchronized block to guarantee consistency when manipulating list of pointers.
    private final Lock pointerManipulationLock = new ReentrantLock();

    protected Logger getLogger()
    {
        return logger;
    }

    /**
     * Static constructor.
     * @param capacity : size in byte of the internal buffer
     * @param bufferNumber : arbitrary number of the buffer.
     * @return an OffHeapMemoryBuffer with internal buffer allocated.
     */
    public static OffHeapMergingMemoryBufferImpl createNew( int capacity, int bufferNumber )
    {
        logger.info( Format.it( "Creating OffHeapLinkedMemoryBuffer %d with a capacity of %s", bufferNumber,
                                Ram.inMb( capacity ) ) );
        return new OffHeapMergingMemoryBufferImpl( ByteBuffer.allocateDirect( capacity ), bufferNumber );
    }

    /**
     * Static constructor. bufferNumber will be set to -1.
     * @param capacity : size in byte of the internal buffer
     * @return an OffHeapMemoryBuffer with internal buffer allocated.
     */
    public static OffHeapMergingMemoryBufferImpl createNew( int capacity )
    {
        return new OffHeapMergingMemoryBufferImpl( ByteBuffer.allocateDirect( capacity ), -1 );
    }

    /**
     * Constructor.
     * @param buffer : the internal buffer
     * @param bufferNumber : arbitrary number of the buffer.
     */
    private OffHeapMergingMemoryBufferImpl( ByteBuffer buffer, int bufferNumber )
    {
        super( buffer, bufferNumber );
        createAndAddFirstPointer();
    }

    /**
     * Initialization function. Create an initial free {@link Pointer} mapping the whole buffer.
     */
    protected Pointer createAndAddFirstPointer()
    {
        Pointer first = new Pointer( 0, buffer.capacity() - 1 );
        first.bufferNumber = bufferNumber;
        first.free = true;
        freePointersBySizeDesc.put( first, DEFAULT_VALUE );
        freePointersByMemoryOffsetAsc.put( first, DEFAULT_VALUE );
        return first;
    }

    protected Pointer firstMatch( int capacity )
    {
        // check for empty instead of throwing an exception.
        if ( freePointersBySizeDesc.isEmpty() )
        {
            return null;
        }
        try
        {
            Pointer ptr = freePointersBySizeDesc.firstKey();

            if ( ptr.getCapacity() >= capacity )
            { // 0 indexed
                return ptr;
            }
        }
        catch ( NoSuchElementException e )
        {
            // noop, just return null at the end of the function.
        }
        return null;
    }

    @Override
    public byte[] retrieve( Pointer pointer )
    {
        pointer.lastHit = System.currentTimeMillis();
        pointer.hits++;

        ByteBuffer buf = buffer.duplicate();
        buf.position( pointer.start );

        final byte[] swp = new byte[pointer.getCapacity()];
        buf.get( swp );
        return swp;
    }

    @Override
    public int free( Pointer pointer2free )
    {

        // Avoid freeing twice the same pointer. Maybe atomic boolean is required here.
        if ( !pointer2free.free )
        {

            try
            {
                pointerManipulationLock.lock();

                if ( !usedPointers.remove( pointer2free ) )
                {
                    return 0;
                }

                Pointer lowerPointerToMerge = pointer2free;

                // search for adjacent pointers lower than the current one 
                for ( Pointer adjacentPointer : freePointersByMemoryOffsetAsc.headMap( pointer2free, false )
                    .descendingKeySet() )
                {

                    if ( adjacentPointer.end + 1 != lowerPointerToMerge.start )
                    {
                        break;
                    }

                    lowerPointerToMerge = adjacentPointer;
                }

                Pointer higherPointerToMerge = pointer2free;

                // search for adjacent pointers higher than the current one
                for ( Pointer adjacentPointer : freePointersByMemoryOffsetAsc.tailMap( pointer2free, false )
                    .navigableKeySet() )
                {

                    if ( adjacentPointer.start - 1 != higherPointerToMerge.end )
                    {
                        break;
                    }

                    higherPointerToMerge = adjacentPointer;
                }

                // if there is adjacent pointers
                if ( lowerPointerToMerge != higherPointerToMerge )
                {

                    final Pointer mergedPointer = new Pointer( lowerPointerToMerge.start, higherPointerToMerge.end );
                    mergedPointer.free = true;

                    final Iterator<Pointer> adjacentPointersIterator = freePointersByMemoryOffsetAsc
                        .subMap( lowerPointerToMerge, true, higherPointerToMerge, true ).navigableKeySet().iterator();
                    while ( adjacentPointersIterator.hasNext() )
                    {
                        Pointer adjacentPointer = adjacentPointersIterator.next();
                        adjacentPointer.free = true; // if a reference to the pointer is kept, we must not use it.
                        freePointersBySizeDesc.remove( adjacentPointer );
                        adjacentPointersIterator.remove();
                    }

                    freePointersByMemoryOffsetAsc.put( mergedPointer, DEFAULT_VALUE );
                    freePointersBySizeDesc.put( mergedPointer, DEFAULT_VALUE );
                }
                else
                {
                    freePointersByMemoryOffsetAsc.put( pointer2free, DEFAULT_VALUE );
                    freePointersBySizeDesc.put( pointer2free, DEFAULT_VALUE );
                }

            }
            finally
            {
                pointerManipulationLock.unlock();
            }

            resetPointer( pointer2free );

            final int pointerCapacity = pointer2free.getCapacity();
            used.addAndGet( -pointerCapacity );

            return pointerCapacity;
        }
        else
        {
            return 0;
        }
    }

    /**
     * Clear the buffer. Free all used pointers, clear all structures and create a new initial pointer.
     */
    @Override
    public void clear()
    {
        allocationErrors = 0;

        for ( Pointer pointer : usedPointers )
        {
            pointer.free = true;
            //            free( pointer ); // too costly to merge every pointers while the will be cleared in a row
        }
        usedPointers.clear();
        freePointersByMemoryOffsetAsc.clear();
        freePointersBySizeDesc.clear();

        createAndAddFirstPointer();
        buffer.clear();
        used.set( 0 );
    }

    @Override
    protected Pointer store( byte[] payload, long expiresIn, long expires )
    {
        final int size = payload.length;

        final Pointer allocatedPointer = allocatePointer( size );

        if ( allocatedPointer != null )
        {

            setExpiration( allocatedPointer, expiresIn, expires );

            allocatedPointer.free = false;

            final ByteBuffer buf = buffer.duplicate();
            buf.position( allocatedPointer.start );
            buf.limit( allocatedPointer.start + size );

            buf.put( payload );

            used.addAndGet( size );

        }

        return allocatedPointer;
    }

    private Pointer allocatePointer( int size )
    {

        Pointer goodOne, fresh = null;

        try
        {

            pointerManipulationLock.lock();

            goodOne = firstMatch( size );

            if ( goodOne == null )
            {
                allocationErrors++;
                return null; // not enough space on this buffer. 
            }

            // Remove good pointer because it's size and offset will change.
            freePointersByMemoryOffsetAsc.remove( goodOne );
            freePointersBySizeDesc.remove( goodOne );

            //fresh = slice(goodOne, size);
            fresh = goodOne;

            if ( goodOne.getCapacity() != size )
            {

                fresh = new Pointer( goodOne.start, goodOne.start + size - 1 );
                fresh.bufferNumber = getBufferNumber();
                fresh.free = true;
                fresh.created = System.currentTimeMillis();

                // create a new pointer for the remaining space 
                final Pointer newGoodOne = new Pointer( fresh.end + 1, goodOne.end );
                newGoodOne.free = true;

                // and add it to the free lists
                freePointersByMemoryOffsetAsc.put( newGoodOne, DEFAULT_VALUE );
                freePointersBySizeDesc.put( newGoodOne, DEFAULT_VALUE );
            }

            usedPointers.add( fresh );

        }
        finally
        {
            pointerManipulationLock.unlock();
        }

        return fresh;

    }

    @Override
    public Pointer allocate( int size, long expiresIn, long expires )
    {

        final Pointer allocatedPointer = allocatePointer( size );

        if ( allocatedPointer != null )
        {
            setExpiration( allocatedPointer, expiresIn, expires );

            allocatedPointer.free = false;

            final ByteBuffer buf = buffer.duplicate();
            buf.position( allocatedPointer.start );
            buf.limit( allocatedPointer.start + size );
            allocatedPointer.directBuffer = buf.slice();

            used.addAndGet( size );
        }

        return allocatedPointer;
    }

    /**
     * Sort {@link Pointer}s by size desc.
     */
    private static class PointerBySizeDesc
        implements Comparator<Pointer>
    {

        @Override
        public int compare( final Pointer pointer0, final Pointer pointer1 )
        {
            final int size0 = pointer0.getCapacity();
            final int size1 = pointer1.getCapacity();

            if ( size0 > size1 )
            {
                return -1;
            }
            else
            {
                if ( size0 == size1 )
                {
                    return 0;
                }
                else
                {
                    return 1;
                }
            }
        }
    }

    /**
     * Sort {@link Pointer}s by memory offset asc.
     */
    private static class PointerByMemoryOffsetAsc
        implements Comparator<Pointer>
    {

        @Override
        public int compare( final Pointer pointer0, final Pointer pointer1 )
        {
            final int offset0 = pointer0.start;
            final int offset1 = pointer1.start;

            if ( offset0 < offset1 )
            {
                return -1;
            }
            else
            {
                if ( offset0 == offset1 )
                {
                    return 0;
                }
                else
                {
                    return 1;
                }
            }
        }
    }

    @Override
    protected List<Pointer> getUsedPointers()
    {
        return new ArrayList<Pointer>( usedPointers );
    }

    @Override
    public Pointer update( Pointer pointer, byte[] payload )
    {
        if ( payload.length > pointer.getCapacity() )
        {
            throw new BufferOverflowException();
        }
        // Create an independent view of the buffer
        final ByteBuffer buf = buffer.duplicate();
        // Set it at the right start offset
        buf.position( pointer.start );
        // Write the content in the shared buffer
        buf.put( payload );

        return pointer;
    }

    public List<Pointer> getPointers()
    {
        // TODO : remove this conversion from Set to List ...
        return new ArrayList<Pointer>( usedPointers );
    }
}
