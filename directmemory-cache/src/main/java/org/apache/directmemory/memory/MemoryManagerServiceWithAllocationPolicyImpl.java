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

/**
 * PoC of {@link MemoryManagerService} that allows {@link AllocationPolicy} to get wired.
 *
 * @author bperroud
 */
public class MemoryManagerServiceWithAllocationPolicyImpl
    extends MemoryManagerServiceImpl
{

    protected AllocationPolicy allocationPolicy;

    @Override
    public void init( int numberOfBuffers, int size )
    {
        super.init( numberOfBuffers, size );
        allocationPolicy.init( getBuffers() );
    }

    public void setAllocationPolicy( final AllocationPolicy allocationPolicy )
    {
        this.allocationPolicy = allocationPolicy;
    }

    @Override
    public OffHeapMemoryBuffer getActiveBuffer()
    {
        return allocationPolicy.getActiveBuffer( null, 0 );
    }

    @Override
    public Pointer store( byte[] payload, int expiresIn )
    {
        Pointer p = null;
        OffHeapMemoryBuffer buffer = null;
        int allocationNumber = 1;
        do
        {
            buffer = allocationPolicy.getActiveBuffer( buffer, allocationNumber );
            if ( buffer == null )
            {
                return null;
            }
            p = buffer.store( payload, expiresIn );
            allocationNumber++;
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
    public Pointer allocate( int size, long expiresIn, long expires )
    {
        Pointer p = null;
        OffHeapMemoryBuffer buffer = null;
        int allocationNumber = 1;
        do
        {
            buffer = allocationPolicy.getActiveBuffer( buffer, allocationNumber );
            if ( buffer == null )
            {
                return null;
            }
            p = buffer.allocate( size, expiresIn, expires );
            allocationNumber++;
        }
        while ( p == null );
        return p;
    }

}
