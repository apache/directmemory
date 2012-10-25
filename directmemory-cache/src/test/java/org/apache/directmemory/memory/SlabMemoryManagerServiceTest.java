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
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.Collection;
import java.util.HashSet;

import org.apache.directmemory.memory.allocator.Allocator;
import org.apache.directmemory.memory.allocator.FixedSizeByteBufferAllocatorImpl;
import org.apache.directmemory.memory.allocator.SlabByteBufferAllocatorImpl;
import org.junit.Test;

public class SlabMemoryManagerServiceTest
    extends AbstractMemoryManagerServiceTest
{

    @Override
    protected MemoryManagerService<Object> instanciateMemoryManagerService( int bufferSize )
    {
        final MemoryManagerService<Object> mms = new MemoryManagerServiceImpl<Object>() {

            @Override
            protected Allocator instanciateByteBufferAllocator( int allocatorNumber, int size )
            {
                Collection<FixedSizeByteBufferAllocatorImpl> slabs = new HashSet<FixedSizeByteBufferAllocatorImpl>();
                
                slabs.add( new FixedSizeByteBufferAllocatorImpl(0, size, SMALL_PAYLOAD_LENGTH / 2, 1) );
                slabs.add( new FixedSizeByteBufferAllocatorImpl(1, size, SMALL_PAYLOAD_LENGTH, 1) );
                
                final SlabByteBufferAllocatorImpl allocator = new SlabByteBufferAllocatorImpl( allocatorNumber, slabs, false );
                
                return allocator;
            }
            
        };
        mms.init( 1, bufferSize );
        return mms;
    }

    @Override
    @Test
    public void testFullFillAndFreeAndClearBuffer()
    {
        
    }
    
    @Override
    @Test
    public void testStoreAllocAndFree()
    {
        
    }
    
    
    @Override
    @Test
    public void testAllocate()
    {
        
    }
    
}
