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
import java.util.Collections;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * {@link ByteBufferAllocator} implementation that instantiate {@link ByteBuffer}s of fixed size, called slices.
 * 
 * @author bperroud
 * 
 */
public class FixedSizeByteBufferAllocatorImpl
    extends AbstractByteBufferAllocator
{

    protected static Logger logger = LoggerFactory.getLogger( FixedSizeByteBufferAllocatorImpl.class );

    // Collection that keeps track of the parent buffers (segments) where slices are allocated
    private final Set<ByteBuffer> segmentsBuffers = new HashSet<ByteBuffer>();

    // Collection that owns all slice that can be used.
    private final Queue<ByteBuffer> freeBuffers = new ConcurrentLinkedQueue<ByteBuffer>();

    // Size of each slices dividing each segments of the slab
    private final int sliceSize;

    // Total size of the current slab
    private int totalSize;

    // Tells if one need to keep track of borrowed buffers
    private boolean keepTrackOfUsedSliceBuffers = false;
    
    // Tells if it returns null or throw an BufferOverflowExcpetion with the requested size is bigger than the size of the slices
    private boolean returnNullWhenOversizingSliceSize = true;
    
    // Tells if it returns null when no buffers are available
    private boolean returnNullWhenNoBufferAvailable = true;

    // Collection that keeps track of borrowed buffers
    private final Set<ByteBuffer> usedSliceBuffers = Collections
        .newSetFromMap( new ConcurrentHashMap<ByteBuffer, Boolean>() );

    protected Logger getLogger()
    {
        return logger;
    }

    /**
     * Constructor.
     * @param totalSize : the internal buffer
     * @param sliceSize : arbitrary number of the buffer.
     * @param numberOfSegments : 
     */
    FixedSizeByteBufferAllocatorImpl( final int number, final int totalSize, final int sliceSize, final int numberOfSegments )
    {
        super( number );
        
        this.totalSize = totalSize;
        this.sliceSize = sliceSize;

        init( numberOfSegments );

    }

    protected void init( final int numberOfSegments )
    {
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

        if ( keepTrackOfUsedSliceBuffers && !usedSliceBuffers.remove( byteBuffer ) )
        {
            return;
        }

        Preconditions.checkArgument( byteBuffer.capacity() == sliceSize );

        freeBuffers.offer( byteBuffer );

    }

    @Override
    public ByteBuffer allocate( int size )
    {

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

        allocatedByteBuffer.clear();
        allocatedByteBuffer.limit( size );

        if ( keepTrackOfUsedSliceBuffers )
        {
            usedSliceBuffers.add( allocatedByteBuffer );
        }

        return allocatedByteBuffer;

    }

    public int getSliceSize()
    {
        return sliceSize;
    }

    @Override
    public void clear()
    {
        // Nothing to do.
    }

    @Override
    public int getCapacity()
    {
        return totalSize;
    }
    
    @Override
    public void close()
    {
        clear();
        
        for ( final ByteBuffer buffer : segmentsBuffers )
        {
            try 
            {
                DirectByteBufferUtils.destroyDirectByteBuffer( buffer );
            }
            catch (Exception e)
            {
                
            }
        }
    }
}
