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

import org.apache.directmemory.memory.allocator.Allocator;

/**
 * Interface describing the buffer allocation policy.
 * The implementations will be initialized by setting the list of buffers {@link #init(List)},
 * and every allocation will call {@link #getActiveBuffer(OffHeapMemoryBuffer, int)},
 * passing the previously (possibly null) buffer that failed to allocate and the number of the current allocation
 *
 * @author bperroud
 *
 */
public interface AllocationPolicy
{

    /**
     * Initialization function.
     *
     * @param buffers
     */
    void init( List<Allocator> allocators );

    /**
     * Returns the {@link Allocator} to use to allocate.
     *
     * @param previousAllocator : the previously used {@link Allocator}, or null if it's the first allocation
     * @param allocationNumber : the number of time the allocation has already failed.
     * @return the {@link Allocator} to use, or null if allocation has failed.
     */
    Allocator getActiveAllocator( Allocator previousAllocator, int allocationNumber );

    /**
     * Reset internal state
     */
    void reset();

}
