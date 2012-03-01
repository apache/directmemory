package org.apache.directmemory.memory.allocator;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@link ByteBufferAllocator} implementation with {@link ByteBuffer} merging capabilities
 *
 * {@link TreeMap} can be safely used because synchronization is achieved through a {@link Lock}
 *
 * @author bperroud
 *
 */
public class MergingByteBufferAllocatorImpl
    extends AbstractByteBufferAllocator
{

    private static final double DEFAULT_SIZE_RATIO_THRESHOLD = 0.9;

    private static final int DEFAULT_MIN_SIZE_THRESHOLD = 128;
    
    protected static Logger logger = LoggerFactory.getLogger( MergingByteBufferAllocatorImpl.class );

    // List of free pointers, with several list of different size
    private final NavigableMap<Integer, Collection<LinkedByteBuffer>> freePointers = new ConcurrentSkipListMap<Integer, Collection<LinkedByteBuffer>>();

    // Set of used pointers. The key is a hash of ByteBuffer.
    private final Map<Integer, LinkedByteBuffer> usedPointers = new ConcurrentHashMap<Integer, LinkedByteBuffer>();

    // Lock used instead of synchronized block to guarantee consistency when manipulating list of pointers.
    private final Lock pointerManipulationLock = new ReentrantLock();

    private final ByteBuffer parentBuffer;

    // Allowed size ratio of the returned pointer before splitting the pointer
    private double sizeRatioThreshold = DEFAULT_SIZE_RATIO_THRESHOLD;
    
    //
    private double minSizeThreshold = DEFAULT_MIN_SIZE_THRESHOLD;

    private boolean returnNullWhenBufferIsFull = true;
    
    protected Logger getLogger()
    {
        return logger;
    }

    /**
     * Constructor.
     * @param buffer : the internal buffer
     * @param bufferNumber : arbitrary number of the buffer.
     */
    public MergingByteBufferAllocatorImpl( final int number, final int totalSize )
    {
        super( number );
        
        parentBuffer = ByteBuffer.allocateDirect( totalSize );
        init();
    }

    /**
     * Initialization function. Create an initial free {@link Pointer} mapping the whole buffer.
     */
    protected void init()
    {
        Integer totalSize = Integer.valueOf( parentBuffer.capacity() );

        for ( Integer i : generateQueueSizes( totalSize ) )
        {
            freePointers.put( Integer.valueOf( i ), new LinkedHashSet<LinkedByteBuffer>() );
        }

        initFirstBuffer();

    }

    private void initFirstBuffer()
    {
        parentBuffer.clear();
        final ByteBuffer initialBuffer = parentBuffer.slice();
        final LinkedByteBuffer initialLinkedBuffer = new LinkedByteBuffer( 0, initialBuffer, null, null );

        insertLinkedBuffer( initialLinkedBuffer );
    }
    
    
    protected List<Integer> generateQueueSizes( final Integer totalSize )
    {
        List<Integer> sizes = new ArrayList<Integer>();

        for ( int i = 128; i <= totalSize; i *= 8 )
        {
            sizes.add( Integer.valueOf( i ) );
        }

        // If totalSize < 128 or totalSize is not a multiple of 128 
        // we force adding an element to the map
        if ( sizes.isEmpty() || !sizes.contains( totalSize ) )
        {
            sizes.add( totalSize );
        }

        return sizes;
    }

    @Override
    public void free( final ByteBuffer buffer )
    {
        
        LinkedByteBuffer returningLinkedBuffer = usedPointers.remove( getHash( buffer ) );
        
        if ( returningLinkedBuffer == null )
        {
            // Hu ? returned twice ? Not returned at the right place ?
            throw new IllegalArgumentException( "The buffer " + buffer + " seems not to belong to this allocator" );
        }

        try
        {
            pointerManipulationLock.lock();

            if ( returningLinkedBuffer.getBefore() != null )
            {
                // if returningLinkedBuffer.getBefore is in the free list, it is free, then it's free and can be merged
                if (getFreeLinkedByteBufferCollection( returningLinkedBuffer.getBefore() ).contains( returningLinkedBuffer.getBefore() ) )
                {
                    returningLinkedBuffer = mergePointer( returningLinkedBuffer.getBefore(), returningLinkedBuffer );
                }
            }

            if ( returningLinkedBuffer.getAfter() != null )
            {
                // if returningLinkedBuffer.getAfter is in the free list, it is free, it is free, then it's free and can be merged 
                if (getFreeLinkedByteBufferCollection( returningLinkedBuffer.getAfter() ).contains( returningLinkedBuffer.getAfter() ) )
                {
                    returningLinkedBuffer = mergePointer( returningLinkedBuffer, returningLinkedBuffer.getAfter() );
                }
            }

            insertLinkedBuffer( returningLinkedBuffer );
        }
        finally
        {
            pointerManipulationLock.unlock();
        }
    }

    @Override
    public ByteBuffer allocate( final int size )
    {

        try
        {
            pointerManipulationLock.lock();

            final SortedMap<Integer, Collection<LinkedByteBuffer>> freeMap = freePointers
                .tailMap( size - 1 );
            for ( final Map.Entry<Integer, Collection<LinkedByteBuffer>> bufferQueueEntry : freeMap.entrySet() )
            {

                Iterator<LinkedByteBuffer> linkedByteBufferIterator = bufferQueueEntry.getValue().iterator();

                while ( linkedByteBufferIterator.hasNext() )
                {
                    LinkedByteBuffer linkedBuffer = linkedByteBufferIterator.next();

                    if ( linkedBuffer.getBuffer().capacity() >= size )
                    {
                        // Remove this element from the collection
                        linkedByteBufferIterator.remove();

                        LinkedByteBuffer returnedLinkedBuffer = linkedBuffer;

                        // Check if splitting need to be performed
                        if ( linkedBuffer.getBuffer().capacity() > minSizeThreshold
                            && ( 1.0 * size / linkedBuffer.getBuffer().capacity() ) < sizeRatioThreshold )
                        {
                            // Split the buffer in a buffer that will be returned and another buffer reinserted in the corresponding queue.
                            parentBuffer.clear();
                            parentBuffer.position( linkedBuffer.getOffset() );
                            parentBuffer.limit( linkedBuffer.getOffset() + size );
                            final ByteBuffer newBuffer = parentBuffer.slice();

                            returnedLinkedBuffer = new LinkedByteBuffer( linkedBuffer.getOffset(), newBuffer,
                                                                         linkedBuffer.getBefore(), null );
                            
                            if (linkedBuffer.getBefore() != null)
                            {
                                linkedBuffer.getBefore().setAfter( returnedLinkedBuffer );
                            }
                            
                            parentBuffer.clear();
                            parentBuffer.position( linkedBuffer.getOffset() + size );
                            parentBuffer.limit( linkedBuffer.getOffset() + linkedBuffer.getBuffer().capacity() );
                            final ByteBuffer remainingBuffer = parentBuffer.slice();
                            final LinkedByteBuffer remainingLinkedBuffer = new LinkedByteBuffer(
                                         linkedBuffer.getOffset() + size, remainingBuffer,
                                         returnedLinkedBuffer, linkedBuffer.getAfter() );
                            
                            if (linkedBuffer.getAfter() != null)
                            {
                                linkedBuffer.getAfter().setBefore( remainingLinkedBuffer );
                            }
                            
                            returnedLinkedBuffer.setAfter( remainingLinkedBuffer );
                            
                            insertLinkedBuffer( remainingLinkedBuffer );
                            
                        }
                        else
                        {
                            // If the buffer is not split, set the limit accordingly
                            returnedLinkedBuffer.getBuffer().clear();
                            returnedLinkedBuffer.getBuffer().limit( size );
                        }

                        usedPointers.put( getHash( returnedLinkedBuffer.getBuffer() ), returnedLinkedBuffer );

                        return returnedLinkedBuffer.getBuffer();
                    }

                }
            }

            if (returnNullWhenBufferIsFull)
            {
                return null;
            }
            else
            {
                throw new BufferOverflowException();
            }
            
        }
        finally
        {
            pointerManipulationLock.unlock();
        }
    }

    @Override
    public void clear()
    {
        usedPointers.clear();

        for ( final Map.Entry<Integer, Collection<LinkedByteBuffer>> bufferQueueEntry : freePointers.entrySet() )
        {
            bufferQueueEntry.getValue().clear();
        }
        
        initFirstBuffer();
    }

    private void insertLinkedBuffer( final LinkedByteBuffer linkedBuffer )
    {
        getFreeLinkedByteBufferCollection( linkedBuffer ).add( linkedBuffer );
    }

    private Collection<LinkedByteBuffer> getFreeLinkedByteBufferCollection( final LinkedByteBuffer linkedBuffer )
    {
        final Integer size = Integer.valueOf( linkedBuffer.getBuffer().capacity() - 1 );
        final Map.Entry<Integer, Collection<LinkedByteBuffer>> bufferCollectionEntry = freePointers.ceilingEntry( size );
        return bufferCollectionEntry.getValue();
    }

    private LinkedByteBuffer mergePointer( final LinkedByteBuffer first, final LinkedByteBuffer next )
    {
        parentBuffer.clear();
        parentBuffer.position( first.getOffset() );
        parentBuffer.limit( first.getOffset() + first.getBuffer().capacity() + next.getBuffer().capacity() );
        final ByteBuffer newByteBuffer = parentBuffer.slice();

        final LinkedByteBuffer newLinkedByteBuffer = new LinkedByteBuffer( first.getOffset(), newByteBuffer,
                                                                           first.getBefore(), next.getAfter() );

        if ( first.getBefore() != null )
        {
            first.getBefore().setAfter( newLinkedByteBuffer );
        }

        if ( next.getAfter() != null )
        {
            next.getAfter().setBefore( newLinkedByteBuffer );
        }

        // Remove the two pointers from their corresponding free lists.
        getFreeLinkedByteBufferCollection( first ).remove( first );
        getFreeLinkedByteBufferCollection( next ).remove( next );
        
        return newLinkedByteBuffer;
    }

    private static Integer getHash( final ByteBuffer buffer )
    {
        return Integer.valueOf( System.identityHashCode( buffer ) );
    }
    
    public void setSizeRatioThreshold( final double sizeRatioThreshold )
    {
        this.sizeRatioThreshold = sizeRatioThreshold;
    }
    
    public void setMinSizeThreshold( final double minSizeThreshold )
    {
        this.minSizeThreshold = minSizeThreshold;
    }

    public void setReturnNullWhenBufferIsFull( boolean returnNullWhenBufferIsFull )
    {
        this.returnNullWhenBufferIsFull = returnNullWhenBufferIsFull;
    }

    @Override
    public int getCapacity()
    {
        return parentBuffer.capacity();
    }
    
    private static class LinkedByteBuffer
    {
        private final int offset;

        private final ByteBuffer buffer;

        private volatile LinkedByteBuffer before;

        private volatile LinkedByteBuffer after;

        public LinkedByteBuffer( final int offset, final ByteBuffer buffer, final LinkedByteBuffer before,
                                 final LinkedByteBuffer after )
        {
            this.offset = offset;
            this.buffer = buffer;
            setBefore( before );
            setAfter( after );
        }

        public ByteBuffer getBuffer()
        {
            return buffer;
        }

        public int getOffset()
        {
            return offset;
        }

        public LinkedByteBuffer getBefore()
        {
            return before;
        }

        public void setBefore( final LinkedByteBuffer before )
        {
            this.before = before;
        }

        public LinkedByteBuffer getAfter()
        {
            return after;
        }

        public void setAfter( final LinkedByteBuffer after )
        {
            this.after = after;
        }
    }

    @Override
    public void close()
        throws IOException
    {
        clear();
        
        try {
            DirectByteBufferUtils.destroyDirectByteBuffer( parentBuffer );
        }
        catch (Exception e)
        {
            
        }
    }

    
}
