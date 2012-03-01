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

import static java.lang.String.format;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.apache.directmemory.measures.Ram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Deprecated
public class OffHeapMemoryBufferImpl<T>
    extends AbstractOffHeapMemoryBuffer<T>
{

    protected static Logger logger = LoggerFactory.getLogger( OffHeapMemoryBufferImpl.class );

    protected final List<Pointer<T>> pointers = new ArrayList<Pointer<T>>();

    protected Logger getLogger()
    {
        return logger;
    }

    protected List<Pointer<T>> getUsedPointers()
    {
        return pointers;
    }

    public List<Pointer<T>> getPointers()
    {
        return pointers;
    }

    public static <V> OffHeapMemoryBufferImpl<V> createNew( int capacity, int bufferNumber )
    {
        logger.info( format( "Creating OffHeapMemoryBuffer %d with a capacity of %s",
                             bufferNumber, Ram.inMb( capacity ) ) );
        return new OffHeapMemoryBufferImpl<V>( ByteBuffer.allocateDirect( capacity ), bufferNumber );
    }

    public static <V> OffHeapMemoryBufferImpl<V> createNew( int capacity )
    {
        return new OffHeapMemoryBufferImpl<V>( ByteBuffer.allocateDirect( capacity ), -1 );
    }

    protected OffHeapMemoryBufferImpl( ByteBuffer buffer, int bufferNumber )
    {
        super( buffer, bufferNumber );
        createAndAddFirstPointer();
    }

    protected Pointer<T> createAndAddFirstPointer()
    {
        PointerImpl<T> first = new PointerImpl<T>();
        first.bufferNumber = bufferNumber;
        first.start = 0;
        first.free = true;
        first.end = buffer.capacity() - 1;
        pointers.add( first );
        return first;
    }

    protected Pointer<T> slice( Pointer<T> existing, int capacity )
    {
        PointerImpl<T> fresh = new PointerImpl<T>();
        fresh.bufferNumber = existing.getBufferNumber();
        fresh.start = existing.getStart();
        fresh.end = fresh.start + capacity - 1; // 0 indexed
        fresh.free = true;
        existing.setStart( fresh.end + 1 ); // more readable
        return fresh;
    }

    protected Pointer<T> firstMatch( int capacity )
    {
        for ( Pointer<T> ptr : pointers )
        {
            if ( ptr.isFree() && ptr.getCapacity() >= capacity )
            {
                return ptr;
            }
        }
        return null;
    }

    public Pointer<T> store( byte[] payload )
    {
        return store( payload, -1 );
    }

    public byte[] retrieve( Pointer<T> pointer )
    {
        pointer.hit();
        
        ByteBuffer buf = null;
        if ( pointer.getClazz() == ByteBuffer.class )
        {
            buf = pointer.getDirectBuffer();
            buf.position( 0 );
        }
        else
        {
            synchronized ( buffer )
            {
                buf = buffer.duplicate();
                buf.position( pointer.getStart() );
            }
        }

        final byte[] swp = new byte[pointer.getCapacity()];
        buf.get( swp );
        return swp;
    }

    public synchronized int free( Pointer<T> pointer2free )
    {
        resetPointer( pointer2free );
        used.addAndGet( -pointer2free.getCapacity() );
        return pointer2free.getCapacity();
    }

    public synchronized void clear()
    {
        for (final Pointer<T> pointer : pointers)
        {
            pointer.setFree( true );
        }
        allocationErrors = 0;
        pointers.clear();
        createAndAddFirstPointer();
        buffer.clear();
        used.set( 0 );
    }

    public Pointer<T> store( byte[] payload, Date expires )
    {
        return store( payload, 0, expires.getTime() );
    }

    public Pointer<T> store( byte[] payload, long expiresIn )
    {
        return store( payload, expiresIn, 0 );
    }

    protected synchronized Pointer<T> store( byte[] payload, long expiresIn, long expires )
    {
        Pointer<T> goodOne = firstMatch( payload.length );

        if ( goodOne == null )
        {
            allocationErrors++;
            return null;
        }

        Pointer<T> fresh = slice( goodOne, payload.length );

        fresh.createdNow();
        setExpiration( fresh, expiresIn, expires );

        fresh.setFree( false );
        used.addAndGet( payload.length );
        ByteBuffer buf = buffer.slice();
        buf.position( fresh.getStart() );
        try
        {
            buf.put( payload );
        }
        catch ( BufferOverflowException e )
        {
            // RpG not convincing - let's fix it later
            goodOne.setStart( fresh.getStart() );
            goodOne.setEnd( buffer.limit() );
            return null;
        }
        pointers.add( fresh );
        return fresh;
    }

    protected boolean inShortage()
    {
        // a place holder for a more refined version
        return allocationErrors > OffHeapMemoryBufferImpl.maxAllocationErrors;
    }

    // TODO : This function should be put in an Util class.
    public static long crc32( byte[] payload )
    {
        final Checksum checksum = new CRC32();
        checksum.update( payload, 0, payload.length );
        return checksum.getValue();
    }

    public Pointer<T> update( Pointer<T> pointer, byte[] payload )
    {
        free( pointer );
        return store( payload );
    }

    public synchronized <V extends T> Pointer<T> allocate( Class<V> type, int size, long expiresIn, long expires )
    {
        Pointer<T> goodOne = firstMatch( size );

        if ( goodOne == null )
        {
            allocationErrors++;
            return null;
        }

        Pointer<T> fresh = slice( goodOne, size );

        fresh.createdNow();
        setExpiration( fresh, expiresIn, expires );

        fresh.setFree( false );
        used.addAndGet( size );
        ByteBuffer buf = buffer.slice();
        buf.limit( fresh.getStart() + size );
        buf.position( fresh.getStart() );

        fresh.setDirectBuffer( buf.slice() );
        fresh.setClazz( type );
        pointers.add( fresh );
        return fresh;
    }

}
