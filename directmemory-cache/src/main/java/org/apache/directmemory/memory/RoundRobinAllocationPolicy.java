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

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Round Robin allocation policy.
 * An internal counter is incremented (modulo the size of the buffer), 
 * so each calls to {@link #getActiveBuffer(OffHeapMemoryBuffer, int)} 
 * will increment the counter and return the buffer at the index of the counter.
 * 
 * @author bperroud
 *
 */
public class RoundRobinAllocationPolicy
    implements AllocationPolicy
{

    // Increment the counter and get the value. Need to start at -1 to have 0'index at first call.
    private static final int BUFFERS_INDEX_INITIAL_VALUE = -1;
    
    // All the buffers to allocate
    private List<OffHeapMemoryBuffer> buffers;

    // Cyclic counter
    private AtomicInteger buffersIndexCounter = new AtomicInteger( BUFFERS_INDEX_INITIAL_VALUE );

    // Default max number of allocations before returning null buffer. 
    private static final int DEFAULT_MAX_ALLOCATIONS = 2;

    // Current max number of allocations
    private int maxAllocations = DEFAULT_MAX_ALLOCATIONS;

    public void setMaxAllocations( int maxAllocations )
    {
        this.maxAllocations = maxAllocations;
    }

    @Override
    public void setBuffers( List<OffHeapMemoryBuffer> buffers )
    {
        this.buffers = buffers;
    }

    @Override
    public OffHeapMemoryBuffer getActiveBuffer( OffHeapMemoryBuffer previouslyAllocatedBuffer,
                                                             int allocationNumber )
    {
        // If current allocation is more than the limit, return a null buffer.
        if ( allocationNumber > maxAllocations )
        {
            return null;
        }

        // Thread safely increment and get the next buffer's index
        int i = incrementAndGetBufferIndex();

        final OffHeapMemoryBuffer buffer = buffers.get( i );

        return buffer;
    }

    @Override
    public void reset()
    {
        // Reinitialize the counter to it's initial value
        buffersIndexCounter.set( BUFFERS_INDEX_INITIAL_VALUE );
    }

    /**
     * Optimistic (lock free) cyclic (modulo size of the buffer) increment of the counter
     * @return next index
     */
    private int incrementAndGetBufferIndex()
    {
        int newIndex = 0;
        boolean updateOk = false;
        do
        {
            int currentIndex = buffersIndexCounter.get();
            newIndex = ( currentIndex + 1 ) % buffers.size();
            updateOk = buffersIndexCounter.compareAndSet( currentIndex, newIndex );
        }
        while ( !updateOk );
        return newIndex;
    }
}
