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

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.josql.Query;
import org.josql.QueryExecutionException;
import org.josql.QueryParseException;
import org.josql.QueryResults;
import org.slf4j.Logger;

public abstract class AbstractOffHeapMemoryBuffer
    implements OffHeapMemoryBuffer
{

    protected final ByteBuffer buffer;

    protected final AtomicInteger used = new AtomicInteger();

    protected final int bufferNumber;

    protected int allocationErrors = 0;

    public static int maxAllocationErrors = 0;

    protected abstract Logger getLogger();

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
        //		createAndAddFirstPointer();
    }

    protected abstract Pointer createAndAddFirstPointer();

    public Pointer store( byte[] payload )
    {
        return store( payload, -1 );
    }

    protected void freePointer( Pointer pointer2free )
    {
        pointer2free.free = true;
        pointer2free.created = 0;
        pointer2free.lastHit = 0;
        pointer2free.hits = 0;
        pointer2free.expiresIn = 0;
        pointer2free.clazz = null;
        pointer2free.directBuffer = null;
        used.addAndGet( -pointer2free.getCapacity() );
    }

    public Pointer store( byte[] payload, Date expires )
    {
        return store( payload, 0, expires.getTime() );
    }

    public Pointer store( byte[] payload, long expiresIn )
    {
        return store( payload, expiresIn, 0 );
    }

    protected abstract Pointer store( byte[] payload, long expiresIn, long expires );

    protected QueryResults select( String whereClause, List<Pointer> pointers )
        throws QueryParseException, QueryExecutionException
    {
        Query q = new Query();
        q.parse( "SELECT * FROM " + Pointer.class.getCanonicalName() + "  WHERE " + whereClause );
        QueryResults qr = q.execute( pointers );
        return qr;
    }

    protected QueryResults selectOrderBy( String whereClause, String orderBy, String limit, List<Pointer> pointers )
        throws QueryParseException, QueryExecutionException
    {
        Query q = new Query();
        q.parse( "SELECT * FROM " + Pointer.class.getCanonicalName() + "  WHERE " + whereClause + " order by "
            + orderBy + " " + limit );
        QueryResults qr = q.execute( pointers );
        return qr;
    }

    protected boolean inShortage()
    {
        // a place holder for a more refined version
        return allocationErrors > AbstractOffHeapMemoryBuffer.maxAllocationErrors;
    }

    @SuppressWarnings("unchecked")
    protected List<Pointer> filter( final String whereClause, List<Pointer> pointers )
    {
        try
        {
            return select( whereClause, pointers ).getResults();
        }
        catch ( QueryParseException e )
        {
            e.printStackTrace();
        }
        catch ( QueryExecutionException e )
        {
            e.printStackTrace();
        }
        return (List<Pointer>) new ArrayList<Pointer>();
    }

    protected long free( List<Pointer> pointers )
    {
        long howMuch = 0;
        for ( Pointer expired : pointers )
        {
            howMuch += free( expired );
        }
        return howMuch;
    }

    protected abstract List<Pointer> getUsedPointers();

    public void disposeExpiredRelative()
    {
        free( filter( "free=false and expiresIn > 0 and (expiresIn+created) <= " + System.currentTimeMillis(),
                      getUsedPointers() ) );
    }

    public void disposeExpiredAbsolute()
    {
        free( filter( "free=false and expires > 0 and (expires) <= " + System.currentTimeMillis(), getUsedPointers() ) );
    }

    public long collectExpired()
    {
        int limit = 50;
        long disposed = free( filter( "free=false and expiresIn > 0 and (expiresIn+created) <= "
                                          + System.currentTimeMillis() + " limit 1, " + limit, getUsedPointers() ) );
        disposed += free( filter( "free=false and expires > 0 and (expires) <= " + System.currentTimeMillis()
            + " limit 1, 100" + limit, getUsedPointers() ) );
        return disposed;
    }

    public long collectLFU( int limit )
    {
        if ( !inShortage() )
        {
            return 0;
        }
        if ( limit <= 0 )
            limit = getUsedPointers().size() / 10;
        QueryResults qr;
        try
        {
            qr = selectOrderBy( "free=false", "frequency", "limit 1, " + limit, getUsedPointers() );
            @SuppressWarnings("unchecked")
            List<Pointer> result = qr.getResults();
            if ( result.size() > 0 )
            {
                // reset allocation errors if we made some room
                allocationErrors = 0;
            }
            return free( result );
        }
        catch ( QueryParseException e )
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        catch ( QueryExecutionException e )
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return 0;
    }

    public Pointer update( Pointer pointer, byte[] payload )
    {
        if ( payload.length > pointer.getCapacity() )
        {
            throw new BufferOverflowException();
        }
        free( pointer );
        return store( payload );
    }

    public abstract Pointer allocate( int size, long expiresIn, long expires );

    protected void resetPointer( final Pointer pointer ) 
    {
        pointer.free = true;
        pointer.created = 0;
        pointer.lastHit = 0;
        pointer.hits = 0;
        pointer.expiresIn = 0;
        pointer.clazz = null;
        pointer.directBuffer = null;
    }
}
