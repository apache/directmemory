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

import org.apache.directmemory.memory.buffer.MemoryBuffer;

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

/**
 * {@link Allocator} implementation with {@link ByteBuffer} merging capabilities.
 * <p/>
 * {@link ByteBuffer}s are wrapped into an {@link LinkedByteBuffer}, and when a {@link ByteBuffer} is freed,
 * lookup is done to the neighbor to check if they are also free, in which case they are merged.
 * <p/>
 * {@link #setMinSizeThreshold(int)} gives the minimum buffer's size under which no splitting is done.
 * {@link #setSizeRatioThreshold(double)} gives the size ratio (requested allocation / free buffer's size} under which no splitting is done
 * <p/>
 * The free {@link ByteBuffer} are held into a {@link NavigableMap} with keys defining the size's range : 0 -> first key (included), first key -> second key (included), ...
 * Instead of keeping a list of {@link ByteBuffer}s sorted by capacity, {@link ByteBuffer}s in the same size's range are held in the same collection.
 * The size's range are computed by {@link #generateFreeSizesRange(Integer)} and can be overridden.
 *
 * @since 0.6
 */
public class MergingByteBufferAllocatorImpl
    extends AbstractByteBufferAllocator
{

    private static final double DEFAULT_SIZE_RATIO_THRESHOLD = 0.9;

    private static final int DEFAULT_MIN_SIZE_THRESHOLD = 128;

    // List of free pointers, with several lists of different size
    private final NavigableMap<Integer, Collection<LinkedByteBuffer>> freePointers =
        new ConcurrentSkipListMap<Integer, Collection<LinkedByteBuffer>>();

    // Set of used pointers. The key is #getHash(ByteBuffer).
    private final Map<Integer, LinkedByteBuffer> usedPointers = new ConcurrentHashMap<Integer, LinkedByteBuffer>();

    // Lock used instead of synchronized block to guarantee consistency when manipulating list of pointers.
    private final Lock linkedStructureManipulationLock = new ReentrantLock();

    // The initial buffer, from which all the others are sliced
    private final ByteBuffer parentBuffer;

    // Allowed size ratio (requested size / buffer's size) of the returned buffer before splitting
    private double sizeRatioThreshold = DEFAULT_SIZE_RATIO_THRESHOLD;

    // Min size of the returned buffer before splitting
    private int minSizeThreshold = DEFAULT_MIN_SIZE_THRESHOLD;

    // Tells if null is returned or an BufferOverflowException is thrown when the buffer is full
    private boolean returnNullWhenBufferIsFull = true;

    /**
     * Constructor.
     *
     * @param number    : the internal buffer identifier
     * @param totalSize : total size of the parent buffer.
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

        for ( Integer i : generateFreeSizesRange( totalSize ) )
        {
            freePointers.put( Integer.valueOf( i ), new LinkedHashSet<LinkedByteBuffer>() );
        }

        initFirstBuffer();

    }

    /**
     * Create the first {@link LinkedByteBuffer}
     */
    private void initFirstBuffer()
    {
        parentBuffer.clear();
        final ByteBuffer initialBuffer = parentBuffer.slice();
        final LinkedByteBuffer initialLinkedBuffer = new LinkedByteBuffer( 0, initialBuffer, null, null );

        insertLinkedBuffer( initialLinkedBuffer );
    }


    /**
     * Generate free sizes' range. Sizes' range are used to try to allocate the best matching {@ByteBuffer}
     * regarding the requested size. Instead of using a sorted structure, arbitrary size's range are computed.
     *
     * @param totalSize
     * @return a list of all size's level used by the allocator.
     */
    protected List<Integer> generateFreeSizesRange( final Integer totalSize )
    {
        List<Integer> sizes = new ArrayList<Integer>();

        for ( int i = minSizeThreshold; i <= totalSize; i *= 8 )
        {
            sizes.add( Integer.valueOf( i ) );
        }

        // If totalSize < minSizeThreshold or totalSize is not a multiple of minSizeThreshold 
        // we force adding an element to the map
        if ( sizes.isEmpty() || !sizes.contains( totalSize ) )
        {
            sizes.add( totalSize );
        }

        return sizes;
    }

    @Override
    public void free( final MemoryBuffer buffer )
    {
        buffer.free();
    }

    @Override
    public MemoryBuffer allocate( final int size )
    {

        try
        {
            linkedStructureManipulationLock.lock();

            final SortedMap<Integer, Collection<LinkedByteBuffer>> freeMap = freePointers.tailMap( size - 1 );
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

                            returnedLinkedBuffer =
                                new LinkedByteBuffer( linkedBuffer.getOffset(), newBuffer, linkedBuffer.getBefore(),
                                                      null );

                            if ( linkedBuffer.getBefore() != null )
                            {
                                linkedBuffer.getBefore().setAfter( returnedLinkedBuffer );
                            }

                            // Insert the remaining buffer into the structure
                            parentBuffer.clear();
                            parentBuffer.position( linkedBuffer.getOffset() + size );
                            parentBuffer.limit( linkedBuffer.getOffset() + linkedBuffer.getBuffer().capacity() );
                            final ByteBuffer remainingBuffer = parentBuffer.slice();
                            final LinkedByteBuffer remainingLinkedBuffer =
                                new LinkedByteBuffer( linkedBuffer.getOffset() + size, remainingBuffer,
                                                      returnedLinkedBuffer, linkedBuffer.getAfter() );

                            if ( linkedBuffer.getAfter() != null )
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

                        return new MergingNioMemoryBuffer(returnedLinkedBuffer);
                    }

                }
            }

            if ( returnNullWhenBufferIsFull )
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
            linkedStructureManipulationLock.unlock();
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
        final Map.Entry<Integer, Collection<LinkedByteBuffer>> bufferCollectionEntry =
            freePointers.ceilingEntry( size );
        return bufferCollectionEntry.getValue();
    }

    private LinkedByteBuffer mergePointer( final LinkedByteBuffer first, final LinkedByteBuffer next )
    {
        parentBuffer.clear();
        parentBuffer.position( first.getOffset() );
        parentBuffer.limit( first.getOffset() + first.getBuffer().capacity() + next.getBuffer().capacity() );
        final ByteBuffer newByteBuffer = parentBuffer.slice();

        final LinkedByteBuffer newLinkedByteBuffer =
            new LinkedByteBuffer( first.getOffset(), newByteBuffer, first.getBefore(), next.getAfter() );

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

    public void setSizeRatioThreshold( final double sizeRatioThreshold )
    {
        this.sizeRatioThreshold = sizeRatioThreshold;
    }

    public void setMinSizeThreshold( final int minSizeThreshold )
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

    private class MergingNioMemoryBuffer extends NioMemoryBuffer {

        MergingNioMemoryBuffer(LinkedByteBuffer linkedBuffer) {
            super(linkedBuffer.buffer);
        }

        @Override
        public boolean growing() {
            return true;
        }

        @Override
        public void free() {
            LinkedByteBuffer returningLinkedBuffer = usedPointers.remove( getHash( getByteBuffer() ) );

            if ( returningLinkedBuffer == null )
            {
                // Hu ? returned twice ? Not returned at the right place ?
                throw new IllegalArgumentException( "The buffer " + this + " seems not to belong to this allocator" );
            }

            try
            {
                linkedStructureManipulationLock.lock();

                if ( returningLinkedBuffer.getBefore() != null )
                {
                    // if returningLinkedBuffer.getBefore is in the free list, it is free, then it's free and can be merged
                    if ( getFreeLinkedByteBufferCollection( returningLinkedBuffer.getBefore() ).contains(
                            returningLinkedBuffer.getBefore() ) )
                    {
                        returningLinkedBuffer = mergePointer( returningLinkedBuffer.getBefore(), returningLinkedBuffer );
                    }
                }

                if ( returningLinkedBuffer.getAfter() != null )
                {
                    // if returningLinkedBuffer.getAfter is in the free list, it is free, it is free, then it's free and can be merged
                    if ( getFreeLinkedByteBufferCollection( returningLinkedBuffer.getAfter() ).contains(
                            returningLinkedBuffer.getAfter() ) )
                    {
                        returningLinkedBuffer = mergePointer( returningLinkedBuffer, returningLinkedBuffer.getAfter() );
                    }
                }

                insertLinkedBuffer( returningLinkedBuffer );
            }
            finally
            {
                linkedStructureManipulationLock.unlock();
            }
        }
    }

    @Override
    public void close()
        throws IOException
    {
        clear();

        try
        {
            DirectByteBufferUtils.destroyDirectByteBuffer( parentBuffer );
        }
        catch ( Exception e )
        {
            // ignore error as we are on quiet mode here
        }
    }


}
