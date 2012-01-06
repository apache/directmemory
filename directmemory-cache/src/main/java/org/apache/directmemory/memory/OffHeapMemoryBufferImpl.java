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
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.apache.directmemory.measures.Ram;
import org.apache.directmemory.misc.Format;
import org.josql.Query;
import org.josql.QueryExecutionException;
import org.josql.QueryParseException;
import org.josql.QueryResults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OffHeapMemoryBufferImpl
    extends AbstractOffHeapMemoryBuffer
{

    protected static Logger logger = LoggerFactory.getLogger( OffHeapMemoryBufferImpl.class );

    protected final List<Pointer> pointers = new ArrayList<Pointer>();

    protected Logger getLogger()
    {
        return logger;
    }

    protected List<Pointer> getUsedPointers()
    {
        return pointers;
    }

    public List<Pointer> getPointers()
    {
        return pointers;
    }

    public static OffHeapMemoryBufferImpl createNew( int capacity, int bufferNumber )
    {
        logger.info( Format.it( "Creating OffHeapMemoryBuffer %d with a capacity of %s", bufferNumber,
                                Ram.inMb( capacity ) ) );
        return new OffHeapMemoryBufferImpl( ByteBuffer.allocateDirect( capacity ), bufferNumber );
    }

    public static OffHeapMemoryBufferImpl createNew( int capacity )
    {
        return new OffHeapMemoryBufferImpl( ByteBuffer.allocateDirect( capacity ), -1 );
    }

    protected OffHeapMemoryBufferImpl( ByteBuffer buffer, int bufferNumber )
    {
        super( buffer, bufferNumber );
        createAndAddFirstPointer();
    }

    protected Pointer createAndAddFirstPointer()
    {
        Pointer first = new Pointer();
        first.bufferNumber = bufferNumber;
        first.start = 0;
        first.free = true;
        first.end = buffer.capacity() - 1;
        pointers.add( first );
        return first;
    }

    protected Pointer slice( Pointer existing, int capacity )
    {
        Pointer fresh = new Pointer();
        fresh.bufferNumber = existing.bufferNumber;
        fresh.start = existing.start;
        fresh.end = fresh.start + capacity - 1; // 0 indexed
        fresh.free = true;
        existing.start = fresh.end + 1; // more readable
        return fresh;
    }

    protected Pointer firstMatch( int capacity )
    {
        for ( Pointer ptr : pointers )
        {
            if ( ptr.free && ptr.getCapacity() >= capacity )
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

        ByteBuffer buf = null;
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

    public synchronized int free( Pointer pointer2free )
    {
        resetPointer( pointer2free );
        used.addAndGet( -pointer2free.getCapacity() );
        return pointer2free.getCapacity();
    }

    public synchronized void clear()
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

    protected synchronized Pointer store( byte[] payload, long expiresIn, long expires )
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

    protected QueryResults select( String whereClause )
        throws QueryParseException, QueryExecutionException
    {
        Query q = new Query();
        q.parse( "SELECT * FROM " + Pointer.class.getCanonicalName() + "  WHERE " + whereClause );
        QueryResults qr = q.execute( pointers );
        return qr;
    }

    protected QueryResults selectOrderBy( String whereClause, String orderBy, String limit )
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
        return allocationErrors > OffHeapMemoryBufferImpl.maxAllocationErrors;
    }

    @SuppressWarnings("unchecked")
    protected List<Pointer> filter( final String whereClause )
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

    // TODO : This function should be put in an Util class. 
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
