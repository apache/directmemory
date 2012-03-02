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

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

/**
 * {@link ByteBufferAllocator} implementation that instantiate {@link ByteBuffer}s of fixed size, called slices.
 * 
 * @since 0.6
 * 
 */
public class FixedSizeByteBufferAllocatorImpl
    extends AbstractByteBufferAllocator
{

    // Collection that keeps track of the parent buffers (segments) where slices are allocated
    private final Set<ByteBuffer> segmentsBuffers = new HashSet<ByteBuffer>();

    // Collection that owns all slices that can be used.
    private final Queue<ByteBuffer> freeBuffers = new ConcurrentLinkedQueue<ByteBuffer>();

    // Size of each slices dividing each segments of the slab
    private final int sliceSize;

    // Total size of the current slab
    private int totalSize;

    // Tells if it returns null or throw an BufferOverflowException when the requested size is bigger than the size of the slices
    private boolean returnNullWhenOversizingSliceSize = true;
    
    // Tells if it returns null when no buffers are available
    private boolean returnNullWhenNoBufferAvailable = true;

    // Collection that keeps track of borrowed buffers
    private final Map<Integer, ByteBuffer> usedSliceBuffers = new ConcurrentHashMap<Integer, ByteBuffer>();

    
    /**
     * Constructor.
     * @param number : internal identifier of the allocator
     * @param totalSize : the internal buffer
     * @param sliceSize : arbitrary number of the buffer.
     * @param numberOfSegments : number of parent {@link ByteBuffer} to allocate.
     */
    public FixedSizeByteBufferAllocatorImpl( final int number, final int totalSize, final int sliceSize, final int numberOfSegments )
    {
        super( number );
        
        this.totalSize = totalSize;
        this.sliceSize = sliceSize;

        init( numberOfSegments );

    }

    protected void init( final int numberOfSegments )
    {
        checkArgument( numberOfSegments > 0 );
        
        // Compute the size of each segments
        int segmentSize = totalSize / numberOfSegments;
        // size is rounded down to a multiple of the slice size
        segmentSize -= segmentSize % sliceSize;

        for ( int i = 0; i < numberOfSegments; i++ )
        {
            final ByteBuffer segment = ByteBuffer.allocateDirect( segmentSize );
            segmentsBuffers.add( segment );

            for ( int j = 0; j < segment.capacity(); j += sliceSize )
            {
                segment.clear();
                segment.position( j );
                segment.limit( j + sliceSize );
                final ByteBuffer slice = segment.slice();
                freeBuffers.add( slice );
            }
        }
    }
    

    protected ByteBuffer findFreeBuffer( int capacity )
    {
        // ensure the requested size is not bigger than the slices' size
        if ( capacity > sliceSize )
        {
            if (returnNullWhenOversizingSliceSize)
            {
                return null;
            }
            else
            {
                throw new BufferOverflowException();
            }
        }
        // TODO : Add capacity to wait till a given timeout for a freed buffer
        return freeBuffers.poll();
    }

    @Override
    public void free( final ByteBuffer byteBuffer )
    {

        checkState( !isClosed() );
        
        if ( usedSliceBuffers.remove( getHash( byteBuffer ) ) == null )
        {
            return;
        }

        // Ensure the buffer belongs to this slab
        checkArgument( byteBuffer.capacity() == sliceSize );

        freeBuffers.offer( byteBuffer );

    }

    @Override
    public ByteBuffer allocate( int size )
    {

        checkState( !isClosed() );
        
        ByteBuffer allocatedByteBuffer = findFreeBuffer( size );

        if ( allocatedByteBuffer == null )
        {
            if (returnNullWhenNoBufferAvailable)
            {
                return null;
            }
            else
            {
                throw new BufferOverflowException();
            }
        }

        // Reset buffer's state
        allocatedByteBuffer.clear();
        allocatedByteBuffer.limit( size );

        usedSliceBuffers.put( getHash( allocatedByteBuffer ), allocatedByteBuffer );

        return allocatedByteBuffer;

    }

    public int getSliceSize()
    {
        return sliceSize;
    }

    @Override
    public void clear()
    {
        for ( final Map.Entry<Integer, ByteBuffer> entry : usedSliceBuffers.entrySet() )
        {
            freeBuffers.offer( entry.getValue() );
        }
        usedSliceBuffers.clear();
    }

    @Override
    public int getCapacity()
    {
        return totalSize;
    }
    
    @Override
    public void close()
    {
        checkState( !isClosed() );
        
        setClosed( true );
        
        clear();
        
        for ( final ByteBuffer buffer : segmentsBuffers )
        {
            try 
            {
                DirectByteBufferUtils.destroyDirectByteBuffer( buffer );
            }
            catch (Exception e)
            {
                getLogger().warn( "Exception thrown while closing the allocator", e );
            }
        }
    }
}
