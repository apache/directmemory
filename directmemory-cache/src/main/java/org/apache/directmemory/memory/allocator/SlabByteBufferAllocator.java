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
import java.util.Collection;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;


/**
 * {@link Allocator} implementation that uses {@link FixedSizeByteBufferAllocatorImpl}
 * of different size to allocate best matching's size {@link ByteBuffer}
 *
 * @since 0.6
 */
public class SlabByteBufferAllocator
    extends AbstractByteBufferAllocator
{

    // Tells if it returns null when no buffers are available
    private final boolean returnNullWhenNoBufferAvailable = true;

    // Internal slabs sorted by sliceSize
    private final NavigableMap<Long, FixedSizeByteBufferAllocatorImpl> slabs =
        new ConcurrentSkipListMap<Long, FixedSizeByteBufferAllocatorImpl>();

    // Tells if it is allowed to look in a bigger slab to perform the request.
    private final boolean allowAllocationToBiggerSlab;

    /**
     * Constructor.
     *
     * @param number                      : internal allocator identifier
     * @param slabs                       : {@link FixedSizeByteBufferAllocatorImpl} to use for allocation
     * @param allowAllocationToBiggerSlab : tells if it is allowed to look in a bigger slab to perform the request.
     */
    public SlabByteBufferAllocator( final int number, final Collection<FixedSizeByteBufferAllocatorImpl> slabs,
                                    final boolean allowAllocationToBiggerSlab )
    {
        super( number );

        this.allowAllocationToBiggerSlab = allowAllocationToBiggerSlab;

        for ( FixedSizeByteBufferAllocatorImpl slab : slabs )
        {
            this.slabs.put( (long) slab.getSliceSize(), slab );
        }

    }


    /**
     * @param size
     * @return the slab that best match the given size
     */
    private FixedSizeByteBufferAllocatorImpl getSlabThatMatchTheSize( final long size )
    {
        // Find the slab that can carry the wanted size. -1 is used because higherEntry returns a strictly higher entry.
        final Map.Entry<Long, FixedSizeByteBufferAllocatorImpl> entry = slabs.higherEntry( size - 1 );

        if ( entry != null )
        {
            return entry.getValue();
        }

        // If an entry has not been found, this means that no slabs has bigger enough slices to allocate the given size
        return null;
    }

    @Override
    public void free( final MemoryBuffer buffer )
    {

        buffer.free();
        final FixedSizeByteBufferAllocatorImpl slab = getSlabThatMatchTheSize( buffer.capacity() );

        if ( slab == null )
        {
            // Hu ? where this bytebuffer come from ??
            return;
        }

        slab.free( buffer );

    }

    @Override
    public MemoryBuffer allocate( final int size )
    {

        final FixedSizeByteBufferAllocatorImpl slab = getSlabThatMatchTheSize( size );

        if ( slab == null )
        {
            // unable to store such big objects
            if ( returnNullWhenNoBufferAvailable )
            {
                return null;
            }
            else
            {
                throw new BufferOverflowException();
            }
        }

        // Try to allocate the given size
        final MemoryBuffer byteBuffer = slab.allocate( size );

        // If allocation succeed, return the buffer
        if ( byteBuffer != null )
        {
            return byteBuffer;
        }

        // Otherwise we have the option to allow in a bigger slab.
        if ( !allowAllocationToBiggerSlab )
        {
            if ( returnNullWhenNoBufferAvailable )
            {
                return null;
            }
            else
            {
                throw new BufferOverflowException();
            }
        }
        else
        {
            // We can try to allocate to a bigger slab.
            // size + 1 here because getSlabThatMatchTheSize do a size -1 and thus will return the same slab
            final int biggerSize = slab.getSliceSize() + 1;
            final FixedSizeByteBufferAllocatorImpl biggerSlab = getSlabThatMatchTheSize( biggerSize );
            if ( biggerSlab == null )
            {
                // We were already trying to allocate in the biggest slab
                if ( returnNullWhenNoBufferAvailable )
                {
                    return null;
                }
                else
                {
                    throw new BufferOverflowException();
                }
            }

            final MemoryBuffer secondByteBuffer = biggerSlab.allocate( size );

            if ( secondByteBuffer == null )
            {
                if ( returnNullWhenNoBufferAvailable )
                {
                    return null;
                }
                else
                {
                    throw new BufferOverflowException();
                }
            }
            else
            {
                return secondByteBuffer;
            }
        }
    }

    @Override
    public void clear()
    {
        for ( final Map.Entry<Long, FixedSizeByteBufferAllocatorImpl> entry : slabs.entrySet() )
        {
            entry.getValue().clear();
        }
    }

    @Override
    public int getCapacity()
    {
        int totalSize = 0;
        for ( final Map.Entry<Long, FixedSizeByteBufferAllocatorImpl> entry : slabs.entrySet() )
        {
            totalSize += entry.getValue().getCapacity();
        }
        return totalSize;
    }

    @Override
    public void close()
        throws IOException
    {
        for ( final Map.Entry<Long, FixedSizeByteBufferAllocatorImpl> entry : slabs.entrySet() )
        {
            entry.getValue().close();
        }
    }
}
