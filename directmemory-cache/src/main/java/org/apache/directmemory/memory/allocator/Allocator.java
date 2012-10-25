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

import java.io.Closeable;

/**
 * Interface defining interaction with {@link MemoryBuffer}
 * 
 * @since 0.6
 */
public interface Allocator
    extends Closeable
{
    
    /**
     * Returns the given {@link MemoryBuffer} making it available for a future usage. Returning twice a {@link MemoryBuffer} won't throw an exception.
     * @param memoryBuffer : the {@link MemoryBuffer} to return
     */
    void free( final MemoryBuffer memoryBuffer );
    
    /**
     * Allocates and returns a {@link MemoryBuffer} with {@link MemoryBuffer#capacity()} set to the given size.
     * When the allocation fails, it returns either null or throws an {@link BufferOverflowException}, depending on the implementation. 
     * @param size : the size in byte to allocate
     * @return a {@link MemoryBuffer} of the given size, or either return null or throw an {@link BufferOverflowException} when the allocation fails.
     */
    MemoryBuffer allocate( final int size );
    
    /**
     * Clear all allocated {@link MemoryBuffer}, resulting in a empty and ready to deserve {@link Allocator}
     */
    void clear();
    
    /**
     * @return the internal total size that can be allocated 
     */
    int getCapacity();
    
    /**
     * @return the internal identifier of the {@link Allocator}
     */
    int getNumber();
    
}
