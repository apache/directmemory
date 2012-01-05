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

import org.apache.directmemory.measures.Ram;
import org.apache.directmemory.misc.Format;
import org.josql.Query;
import org.josql.QueryExecutionException;
import org.josql.QueryParseException;
import org.josql.QueryResults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

public class OffHeapMemoryBuffer
{
    private static Logger logger = LoggerFactory.getLogger( OffHeapMemoryBuffer.class );

    protected ByteBuffer buffer;

    //TODO: Variable 'pointers' must be private and have accessor methods.
    public List<Pointer> pointers = new ArrayList<Pointer>();

    private AtomicInteger used = new AtomicInteger();

    public int bufferNumber;

    public int allocationErrors = 0;

    public static int maxAllocationErrors = 0;

    public int used()
    {
        return used.get();
    }

    public int capacity()
    {
        return buffer.capacity();
    }

    public static OffHeapMemoryBuffer createNew( int capacity, int bufferNumber )
    {
        logger.info(
            Format.it( "Creating OffHeapMemoryBuffer %d with a capacity of %s", bufferNumber, Ram.inMb( capacity ) ) );
        return new OffHeapMemoryBuffer( ByteBuffer.allocateDirect( capacity ), bufferNumber );
    }

    public static OffHeapMemoryBuffer createNew( int capacity )
    {
        return new OffHeapMemoryBuffer( ByteBuffer.allocateDirect( capacity ), -1 );
    }

    private OffHeapMemoryBuffer( ByteBuffer buffer, int bufferNumber )
    {
        this.buffer = buffer;
        this.bufferNumber = bufferNumber;
        createAndAddFirstPointer();
    }

    private Pointer createAndAddFirstPointer()
    {
        Pointer first = new Pointer();
        first.bufferNumber = bufferNumber;
        first.start = 0;
        first.free = true;
        first.end = buffer.capacity() - 1;
        pointers.add( first );
        return first;
    }

    public Pointer slice( Pointer existing, int capacity )
    {
        Pointer fresh = new Pointer();
        fresh.bufferNumber = existing.bufferNumber;
        fresh.start = existing.start;
        fresh.end = fresh.start + capacity - 1; // 0 indexed
        fresh.free = true;
        existing.start = fresh.end + 1; // more readable
        return fresh;
    }


    public Pointer firstMatch( int capacity )
    {
        for ( Pointer ptr : pointers )
        {
            if (ptr.free && ptr.getCapacity() >= capacity)
            {
                return ptr;
            }
        }
        return null;
    }

    public Pointer store( byte[] payload )
    {
        return store( payload, -1 );
    }

    public byte[] retrieve( Pointer pointer )
    {
        pointer.lastHit = System.currentTimeMillis();
        pointer.hits++;

        ByteBuffer buf;
        if ( pointer.clazz == ByteBuffer.class )
        {
            buf = pointer.directBuffer;
            buf.position( 0 );
        }
        else
        {
            synchronized ( buffer )
            {
                buf = buffer.duplicate();
                buf.position( pointer.start );
            }
        }

        final byte[] swp = new byte[pointer.getCapacity()];
        buf.get( swp );
        return swp;
    }


    public int free( Pointer pointer2free )
    {
        pointer2free.free = true;
        pointer2free.created = 0;
        pointer2free.lastHit = 0;
        pointer2free.hits = 0;
        pointer2free.expiresIn = 0;
        pointer2free.clazz = null;
        pointer2free.directBuffer = null;
        used.addAndGet( - pointer2free.getCapacity() );
        return pointer2free.getCapacity();
    }

    public void clear()
    {
        allocationErrors = 0;
        pointers.clear();
        createAndAddFirstPointer();
        buffer.clear();
        used.set( 0 );
    }

    public Pointer store( byte[] payload, Date expires )
    {
        return store( payload, 0, expires.getTime() );
    }

    public Pointer store( byte[] payload, long expiresIn )
    {
        return store( payload, expiresIn, 0 );
    }

    private synchronized Pointer store( byte[] payload, long expiresIn, long expires )
    {
        Pointer goodOne = firstMatch( payload.length );

        if ( goodOne == null )
        {
            allocationErrors++;
            return null;
        }

        Pointer fresh = slice( goodOne, payload.length );

        fresh.created = System.currentTimeMillis();
        if ( expiresIn > 0 )
        {
            fresh.expiresIn = expiresIn;
            fresh.expires = 0;
        }
        else if ( expires > 0 )
        {
            fresh.expiresIn = 0;
            fresh.expires = expires;
        }

        fresh.free = false;
        used.addAndGet( payload.length );
        ByteBuffer buf = buffer.slice();
        buf.position( fresh.start );
        try
        {
            buf.put( payload );
        }
        catch ( BufferOverflowException e )
        {
            // RpG not convincing - let's fix it later
            goodOne.start = fresh.start;
            goodOne.end = buffer.limit();
            return null;
        }
        pointers.add( fresh );
        return fresh;
    }

    private QueryResults select( String whereClause )
        throws QueryParseException, QueryExecutionException
    {
        Query q = new Query();
        q.parse( "SELECT * FROM " + Pointer.class.getCanonicalName() + "  WHERE " + whereClause );
        return q.execute( pointers );
    }

    private QueryResults selectOrderBy( String whereClause, String orderBy, String limit )
        throws QueryParseException, QueryExecutionException
    {
        Query q = new Query();
        q.parse( "SELECT * FROM " + Pointer.class.getCanonicalName() + "  WHERE " + whereClause + " order by " + orderBy
                     + " " + limit );
        return q.execute( pointers );
    }

    private boolean inShortage()
    {
        // a place holder for a more refined version
        return allocationErrors > OffHeapMemoryBuffer.maxAllocationErrors;
    }

    public long collectLFU( int limit )
    {
        if ( !inShortage() )
        {
            return 0;
        }
        if ( limit <= 0 )
        {
            limit = pointers.size() / 10;
        }
        QueryResults qr;
        try
        {
            qr = selectOrderBy( "free=false", "frequency", "limit 1, " + limit );
            @SuppressWarnings( "unchecked" ) List<Pointer> result = qr.getResults();
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


    @SuppressWarnings( "unchecked" )
    private List<Pointer> filter( final String whereClause )
    {
        try
        {
            return select( whereClause ).getResults();
        }
        catch ( QueryParseException e )
        {
            e.printStackTrace();
        }
        catch ( QueryExecutionException e )
        {
            e.printStackTrace();
        }
        return new ArrayList<Pointer>();
    }

    private long free( List<Pointer> expiredPointers )
    {
        long howMuch = 0;
        for ( Pointer expired : expiredPointers )
        {
            howMuch += free( expired );
        }
        return howMuch;
    }

    public void disposeExpiredRelative()
    {
        free( filter( "free=false and expiresIn > 0 and (expiresIn+created) <= " + System.currentTimeMillis() ) );
    }

    public void disposeExpiredAbsolute()
    {
        free( filter( "free=false and expires > 0 and (expires) <= " + System.currentTimeMillis() ) );
    }

    public long collectExpired()
    {
        int limit = 50;
        long disposed = free( filter(
            "free=false and expiresIn > 0 and (expiresIn+created) <= " + System.currentTimeMillis() + " limit 1, "
                + limit ) );
        disposed += free( filter(
            "free=false and expires > 0 and (expires) <= " + System.currentTimeMillis() + " limit 1, 100" + limit ) );
        return disposed;
    }

    public static long crc32( byte[] payload )
    {
        final Checksum checksum = new CRC32();
        checksum.update( payload, 0, payload.length );
        return checksum.getValue();
    }

    public Pointer update( Pointer pointer, byte[] payload )
    {
        free( pointer );
        return store( payload );
    }

    public synchronized Pointer allocate( int size, long expiresIn, long expires )
    {
        Pointer goodOne = firstMatch( size );

        if ( goodOne == null )
        {
            allocationErrors++;
            return null;
        }

        Pointer fresh = slice( goodOne, size );

        fresh.created = System.currentTimeMillis();
        if ( expiresIn > 0 )
        {
            fresh.expiresIn = expiresIn;
            fresh.expires = 0;
        }
        else if ( expires > 0 )
        {
            fresh.expiresIn = 0;
            fresh.expires = expires;
        }

        fresh.free = false;
        used.addAndGet( size );
        ByteBuffer buf = buffer.slice();
        buf.position( fresh.start );

        fresh.directBuffer = buf.slice();
        fresh.directBuffer.limit( size );
        fresh.clazz = ByteBuffer.class;
        pointers.add( fresh );
        return fresh;
    }

}
