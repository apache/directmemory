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

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

import junit.framework.Assert;

import org.junit.Test;

public class MergingByteBufferAllocatorImplTest
{
    @Test
    public void allocationTest()
    {
        
        ByteBufferAllocator allocator = new MergingByteBufferAllocatorImpl( 0, 5000 );
        
        ByteBuffer bf1 = allocator.allocate( 250 );
        Assert.assertEquals( 250, bf1.capacity() );
        Assert.assertEquals( 250, bf1.limit() );
        
        ByteBuffer bf2 = allocator.allocate( 251 );
        Assert.assertEquals( 251, bf2.capacity() );
        Assert.assertEquals( 251, bf2.limit() );
        
        ByteBuffer bf3 = allocator.allocate( 200 );
        Assert.assertEquals( 200, bf3.capacity() );
        Assert.assertEquals( 200, bf3.limit() );
        
        ByteBuffer bf4 = allocator.allocate( 2000 );
        Assert.assertEquals( 2000, bf4.capacity() );
        Assert.assertEquals( 2000, bf4.limit() );
        
        ByteBuffer bf5 = allocator.allocate( 2001 );
        Assert.assertEquals( 2001, bf5.capacity() );
        Assert.assertEquals( 2001, bf5.limit() );
        
        ByteBuffer bf6 = allocator.allocate( 298 );
        Assert.assertEquals( 298, bf6.capacity() );
        Assert.assertEquals( 298, bf6.limit() );
        
        ByteBuffer bf7 = allocator.allocate( 128 );
        Assert.assertNull( bf7 );
        
    }
    
    
    @Test
    public void releaseTest()
    {
        
        ByteBufferAllocator allocator = new MergingByteBufferAllocatorImpl( 0, 1000 );
        
        ByteBuffer bf1 = allocator.allocate( 250 );
        Assert.assertEquals( 250, bf1.capacity() );
        Assert.assertEquals( 250, bf1.limit() );
        
        ByteBuffer bf2 = allocator.allocate( 251 );
        Assert.assertEquals( 251, bf2.capacity() );
        Assert.assertEquals( 251, bf2.limit() );
        
        ByteBuffer bf3 = allocator.allocate( 252 );
        Assert.assertEquals( 252, bf3.capacity() );
        Assert.assertEquals( 252, bf3.limit() );
        
        ByteBuffer bf4 = allocator.allocate( 500 );
        Assert.assertNull( bf4 );
        
        allocator.free( bf1 );
        allocator.free( bf2 );
        
        ByteBuffer bf5 = allocator.allocate( 500 );
        Assert.assertEquals( 501, bf5.capacity() );
        Assert.assertEquals( 500, bf5.limit() );
        
    }
    
    @Test
    public void allocateAndFreeTest()
    {
        
        ByteBufferAllocator allocator = new MergingByteBufferAllocatorImpl( 0, 1000 );
        
        for (int i = 0; i < 1000; i++)
        {
            ByteBuffer bf1 = allocator.allocate( 250 );
            Assert.assertEquals( 250, bf1.capacity() );
            Assert.assertEquals( 250, bf1.limit() );
            
            allocator.free( bf1 );
        }
        
        
        ByteBuffer bf2 = allocator.allocate( 1000 );
        Assert.assertEquals( 1000, bf2.capacity() );
        Assert.assertEquals( 1000, bf2.limit() );
        
    }
    
    @Test
    public void allocationWithoutSplittingPointerTest()
    {
        
        ByteBufferAllocator allocator = new MergingByteBufferAllocatorImpl( 0, 200 );
        
        ByteBuffer bf1 = allocator.allocate( 180 );
        Assert.assertEquals( 200, bf1.capacity() );
        Assert.assertEquals( 180, bf1.limit() );
        
        ByteBuffer bf2 = allocator.allocate( 5 );
        Assert.assertNull( bf2 );
        
        allocator.free( bf1 );
        
        
        ByteBuffer bf3 = allocator.allocate( 10 );
        Assert.assertEquals( 10, bf3.capacity() );
        Assert.assertEquals( 10, bf3.limit() );
        
        ByteBuffer bf4 = allocator.allocate( 20 );
        Assert.assertEquals( 20, bf4.capacity() );
        Assert.assertEquals( 20, bf4.limit() );
        
        ByteBuffer bf5 = allocator.allocate( 30 );
        Assert.assertEquals( 30, bf5.capacity() );
        Assert.assertEquals( 30, bf5.limit() );
        
        allocator.free( bf4 );
        allocator.free( bf3 );
        
        ByteBuffer bf6 = allocator.allocate( 25 );
        Assert.assertEquals( 30, bf6.capacity() );
        Assert.assertEquals( 25, bf6.limit() );
        
    }
    
    @Test
    public void allocationWithDifferentRatioTest()
    {
        
        MergingByteBufferAllocatorImpl allocator = new MergingByteBufferAllocatorImpl( 0, 200 );
        allocator.setSizeRatioThreshold( 0.95 );
        
        allocator.setSizeRatioThreshold( 10 );
        
        ByteBuffer bf1 = allocator.allocate( 180 );
        Assert.assertEquals( 180, bf1.capacity() );
        Assert.assertEquals( 180, bf1.limit() );
        
        ByteBuffer bf2 = allocator.allocate( 10 );
        Assert.assertEquals( 20, bf2.capacity() );
        Assert.assertEquals( 10, bf2.limit() );
        
    }
    
    
    @Test(expected = BufferOverflowException.class)
    public void allocationThrowingBOExceptionTest()
    {
        
        MergingByteBufferAllocatorImpl allocator = new MergingByteBufferAllocatorImpl( 0, 200 );
        allocator.setReturnNullWhenBufferIsFull( false );
        
        allocator.allocate( 210 );
        Assert.fail();
    }
    
    
}
