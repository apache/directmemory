package org.apache.directmemory.memory;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

import org.apache.directmemory.memory.allocator.ByteBufferAllocator;

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

/**
 * PoC of {@link MemoryManagerService} that allows {@link AllocationPolicy} to get wired.
 *
 * @author bperroud
 */
public class MemoryManagerServiceWithAllocationPolicyImpl<V>
    extends MemoryManagerServiceImpl<V>
{
    
    protected AllocationPolicy allocationPolicy;

    public MemoryManagerServiceWithAllocationPolicyImpl()
    {
        super();
    }
    
    public MemoryManagerServiceWithAllocationPolicyImpl( final AllocationPolicy allocationPolicy, final boolean returnNullWhenFull )
    {
        super( returnNullWhenFull );
        this.setAllocationPolicy( allocationPolicy );
    }
    
    @Override
    public void init( int numberOfBuffers, int size )
    {
        super.init( numberOfBuffers, size );
        allocationPolicy.init( getAllocators() );
    }

    public void setAllocationPolicy( final AllocationPolicy allocationPolicy )
    {
        this.allocationPolicy = allocationPolicy;
    }

    
    protected ByteBufferAllocator getAllocator()
    {
        return allocationPolicy.getActiveAllocator( null, 0 );
    }

    @Override
    public Pointer<V> store( byte[] payload, int expiresIn )
    {
        Pointer<V> p = null;
        ByteBufferAllocator allocator = null;
        int allocationNumber = 0;
        do
        {
            allocationNumber++;
            allocator = allocationPolicy.getActiveAllocator( allocator, allocationNumber );
            if ( allocator == null )
            {
                if (returnsNullWhenFull())
                {
                    return null;
                }
                else
                {
                    throw new BufferOverflowException();
                }
            }
            final ByteBuffer buffer = allocator.allocate( payload.length );
            
            if ( buffer == null )
            {
                continue;
            }
            
            p = instanciatePointer( buffer, allocator.getNumber(), expiresIn, NEVER_EXPIRES );

            buffer.rewind();
            buffer.put( payload );
            
            used.addAndGet( payload.length );
            
        }
        while ( p == null );
        return p;
    }

    @Override
    public void clear()
    {
        super.clear();
        allocationPolicy.reset();
    }

    @Override
    public <T extends V> Pointer<V> allocate( final Class<T> type, final int size, final long expiresIn, final long expires )
    {
        Pointer<V> p = null;
        ByteBufferAllocator allocator = null;
        int allocationNumber = 0;
        do
        {
            allocationNumber++;
            allocator = allocationPolicy.getActiveAllocator( allocator, allocationNumber );
            if ( allocator == null )
            {
                if (returnsNullWhenFull())
                {
                    return null;
                }
                else
                {
                    throw new BufferOverflowException();
                }
            }
            
            final ByteBuffer buffer = allocator.allocate( size );
            
            if ( buffer == null )
            {
                continue;
            }
            
            p = instanciatePointer( buffer, allocator.getNumber(), expiresIn, NEVER_EXPIRES );
            
            used.addAndGet( size );
        }
        while ( p == null );
        
        p.setClazz( type );
        
        return p;
    }

}
