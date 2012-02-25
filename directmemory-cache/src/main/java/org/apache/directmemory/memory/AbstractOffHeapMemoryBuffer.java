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

import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.limit;
import static com.google.common.collect.Ordering.from;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;

import com.google.common.base.Predicate;

public abstract class AbstractOffHeapMemoryBuffer<T>
    implements OffHeapMemoryBuffer<T>
{

    protected final ByteBuffer buffer;

    protected final AtomicInteger used = new AtomicInteger();

    protected final int bufferNumber;

    protected int allocationErrors = 0;

    public static int maxAllocationErrors = 0;

    protected abstract Logger getLogger();

    private final Predicate<Pointer<T>> relative = new Predicate<Pointer<T>>()
    {

        @Override
        public boolean apply( Pointer<T> input )
        {
            return !input.isFree()
                            && !input.isExpired();
        }

    };

    private final Predicate<Pointer<T>> absolute = new Predicate<Pointer<T>>()
    {

        @Override
        public boolean apply( Pointer<T> input )
        {
            return !input.isFree()
                            && !input.isExpired();
        }

    };

    public int used()
    {
        return used.get();
    }

    public int capacity()
    {
        return buffer.capacity();
    }

    public int getBufferNumber()
    {
        return bufferNumber;
    }

    protected AbstractOffHeapMemoryBuffer( ByteBuffer buffer, int bufferNumber )
    {
        this.buffer = buffer;
        this.bufferNumber = bufferNumber;
        //		createAndAddFirstPointer<T>();
    }

    protected abstract Pointer<T> createAndAddFirstPointer();

    public Pointer<T> store( byte[] payload )
    {
        return store( payload, -1 );
    }

    protected void freePointer( Pointer<T> pointer2free )
    {
        pointer2free.reset();
        used.addAndGet( -pointer2free.getCapacity() );
    }

    public Pointer<T> store( byte[] payload, Date expires )
    {
        return store( payload, 0, expires.getTime() );
    }

    public Pointer<T> store( byte[] payload, long expiresIn )
    {
        return store( payload, expiresIn, 0 );
    }

    protected abstract Pointer<T> store( byte[] payload, long expiresIn, long expires );

    protected boolean inShortage()
    {
        // a place holder for a more refined version
        return allocationErrors > AbstractOffHeapMemoryBuffer.maxAllocationErrors;
    }

    protected long free( Predicate<Pointer<T>> predicate )
    {
        return free( filter( getUsedPointers(), predicate ) );
    }

    protected long free( Iterable<Pointer<T>> pointers )
    {
        long howMuch = 0;
        for ( Pointer<T> expired : pointers )
        {
            howMuch += free( expired );
        }
        return howMuch;
    }

    protected abstract List<Pointer<T>> getUsedPointers();

    public void disposeExpiredRelative()
    {
        free( relative );
    }

    public void disposeExpiredAbsolute()
    {
        free( absolute );
    }

    public long collectExpired()
    {
        int limit = 50;
        return free( limit( filter( getUsedPointers(), relative ), limit ) )
                        + free( limit( filter( getUsedPointers(), absolute ), limit ) );
    }

    public long collectLFU( int limit )
    {
        if ( !inShortage() )
        {
            return 0;
        }
        if ( limit <= 0 )
        {
            limit = getUsedPointers().size() / 10;
        }

        Iterable<Pointer<T>> result = from( new Comparator<Pointer<T>>()
        {

            public int compare( Pointer<T> o1, Pointer<T> o2 )
            {
                float f1 = o1.getFrequency();
                float f2 = o2.getFrequency();

                return Float.compare( f1, f2 );
            }

        } ).sortedCopy( limit( filter( getUsedPointers(), new Predicate<Pointer<T>>()
        {

            @Override
            public boolean apply( Pointer<T> input )
            {
                return !input.isFree();
            }

        } ), limit ) );

        if ( result.iterator().hasNext() )
        {
            // reset allocation errors if we made some room
            allocationErrors = 0;
        }
        return free( result );
    }

    public Pointer<T> update( Pointer<T> pointer, byte[] payload )
    {
        if ( payload.length > pointer.getCapacity() )
        {
            throw new BufferOverflowException();
        }
        free( pointer );
        return store( payload );
    }

    protected void resetPointer( final Pointer<T> pointer )
    {
        pointer.reset();
    }

    protected void setExpiration( final Pointer<T> pointer, long expiresIn, long expires )
    {
        if ( expiresIn > 0 )
        {
            pointer.setExpiration( 0, expiresIn );
        }
        else if ( expires > 0 )
        {
            pointer.setExpiration( expires, 0 );
        }
    }

}
