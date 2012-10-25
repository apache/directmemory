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
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.directmemory.memory.buffer.MemoryBuffer;
import org.junit.Test;

public class SlabByteBufferAllocatorImplTest
{
    @Test
    public void allocationTest()
    {
        
        List<FixedSizeByteBufferAllocatorImpl> slabs = new ArrayList<FixedSizeByteBufferAllocatorImpl>();
        slabs.add( new FixedSizeByteBufferAllocatorImpl( 0, 1024, 128, 1 ) );
        slabs.add( new FixedSizeByteBufferAllocatorImpl( 1, 1024, 256, 1 ) );
        slabs.add( new FixedSizeByteBufferAllocatorImpl( 2, 1024, 512, 1 ) );
        slabs.add( new FixedSizeByteBufferAllocatorImpl( 3, 1024, 1024, 1 ) );
        
        
        ByteBufferAllocator allocator = new SlabByteBufferAllocatorImpl( 0, slabs, false );
        
        MemoryBuffer bf1 = allocator.allocate( 250 );
        Assert.assertEquals( 256, bf1.maxCapacity() );
        Assert.assertEquals( 250, bf1.capacity() );
        
        MemoryBuffer bf2 = allocator.allocate( 251 );
        Assert.assertEquals( 256, bf2.maxCapacity() );
        Assert.assertEquals( 251, bf2.capacity() );
        
        MemoryBuffer bf3 = allocator.allocate( 200 );
        Assert.assertEquals( 256, bf3.maxCapacity() );
        Assert.assertEquals( 200, bf3.capacity() );
        
        MemoryBuffer bf4 = allocator.allocate( 100 );
        Assert.assertEquals( 128, bf4.maxCapacity() );
        Assert.assertEquals( 100, bf4.capacity() );
        
        MemoryBuffer bf5 = allocator.allocate( 550 );
        Assert.assertEquals( 1024, bf5.maxCapacity() );
        Assert.assertEquals( 550, bf5.capacity() );
        
        MemoryBuffer bf6 = allocator.allocate( 800 );
        Assert.assertNull( bf6 );

        allocator.free( bf5 );
        
        MemoryBuffer bf7 = allocator.allocate( 800 );
        Assert.assertEquals( 1024, bf7.maxCapacity() );
        Assert.assertEquals( 800, bf7.capacity() );
        
    }
    
}
