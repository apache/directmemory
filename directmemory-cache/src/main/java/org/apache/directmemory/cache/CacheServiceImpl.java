package org.apache.directmemory.cache;

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

import com.google.common.collect.MapMaker;
import org.apache.directmemory.measures.Every;
import org.apache.directmemory.measures.Ram;
import org.apache.directmemory.memory.MemoryManagerService;
import org.apache.directmemory.memory.MemoryManagerServiceImpl;
import org.apache.directmemory.memory.OffHeapMemoryBuffer;
import org.apache.directmemory.memory.Pointer;
import org.apache.directmemory.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentMap;

import static java.lang.String.format;
import static org.apache.directmemory.serialization.SerializerFactory.createNewSerializer;

public class CacheServiceImpl
    implements CacheService
{

    private static Logger logger = LoggerFactory.getLogger( CacheServiceImpl.class );

    private ConcurrentMap<String, Pointer> map;

    private Serializer serializer = createNewSerializer();

    private MemoryManagerService memoryManager = new MemoryManagerServiceImpl();

    private final Timer timer = new Timer();


    /**
     * Constructor
     */
    public CacheServiceImpl()
    {
    }

    /**
     * Constructor
     *
     * @param memoryManager
     */
    public CacheServiceImpl( MemoryManagerService memoryManager )
    {
        this.memoryManager = memoryManager;
    }

    public void scheduleDisposalEvery( long l )
    {
        timer.schedule( new TimerTask()
        {
            public void run()
            {
                logger.info( "begin scheduled disposal" );
                collectExpired();
                collectLFU();
                logger.info( "scheduled disposal complete" );
            }
        }, l );
        logger.info( "disposal scheduled every " + l + " milliseconds" );
    }

    public void init( int numberOfBuffers, int size, int initialCapacity, int concurrencyLevel )
    {
        map = new MapMaker().concurrencyLevel( concurrencyLevel ).initialCapacity( initialCapacity ).makeMap();

        logger.info( "******************************** initializing *******************************" );
        logger.info( "         ____  _                 __  __  ___" );
        logger.info( "        / __ \\(_)________  _____/ /_/  |/  /___  ____ ___  ____  _______  __" );
        logger.info( "       / / / / // ___/ _ \\/ ___/ __/ /|_/ // _ \\/ __ `__ \\/ __ \\/ ___/ / / /" );
        logger.info( "      / /_/ / // /  /  __/ /__/ /_/ /  / //  __/ / / / / / /_/ / /  / /_/ / " );
        logger.info( "     /_____/_//_/   \\___/\\___/\\__/_/  /_/ \\___/_/ /_/ /_/\\____/_/   \\__, /" );
        logger.info( "                                                                   /____/   " );
        logger.info( "********************************************************************************" );

        memoryManager.init( numberOfBuffers, size );

        logger.info( "initialized" );
        logger.info( format( "number of buffer(s): \t%1d  with %2s each", numberOfBuffers, Ram.inMb( size ) ) );
        logger.info( format( "initial capacity: \t%1d", initialCapacity ) );
        logger.info( format( "concurrency level: \t%1d", concurrencyLevel ) );
        scheduleDisposalEvery( Every.seconds( 10 ) );
    }

    public void init( int numberOfBuffers, int size )
    {
        init( numberOfBuffers, size, DEFAULT_INITIAL_CAPACITY, DEFAULT_CONCURRENCY_LEVEL );
    }

    public Pointer putByteArray( String key, byte[] payload )
    {
        return store( key, payload, 0 );
    }

    public Pointer putByteArray( String key, byte[] payload, int expiresIn )
    {
        return store( key, payload, expiresIn );
    }

    public Pointer put( String key, Object object )
    {
        return put( key, object, 0 );
    }

    public Pointer put( String key, Object object, int expiresIn )
    {
        try
        {
            byte[] payload = serializer.serialize( object );
            Pointer ptr = store( key, payload, expiresIn );
            ptr.clazz = object.getClass();
            return ptr;
        }
        catch ( IOException e )
        {
            logger.error( e.getMessage() );
            return null;
        }
    }

    private Pointer store( String key, byte[] payload, int expiresIn )
    {
        Pointer pointer = map.get( key );
        if ( pointer != null )
        {
            return memoryManager.update( pointer, payload );
        }
        else
        {
            pointer = memoryManager.store( payload, expiresIn );
            map.put( key, pointer );
            return pointer;
        }
    }

    public byte[] retrieveByteArray( String key )
    {
        Pointer ptr = getPointer( key );
        if ( ptr == null )
        {
            return null;
        }
        if ( ptr.expired() || ptr.free )
        {
            map.remove( key );
            if ( !ptr.free )
            {
                memoryManager.free( ptr );
            }
            return null;
        }
        else
        {
            return memoryManager.retrieve( ptr );
        }
    }

    public Object retrieve( String key )
    {
        Pointer ptr = getPointer( key );
        if ( ptr == null )
        {
            return null;
        }
        if ( ptr.expired() || ptr.free )
        {
            map.remove( key );
            if ( !ptr.free )
            {
                memoryManager.free( ptr );
            }
            return null;
        }
        else
        {
            if ( ptr.clazz == ByteBuffer.class )
            {
                // skip serialization if it is a bytebuffer
                return ptr.directBuffer;
            }
            try
            {
                return serializer.deserialize( memoryManager.retrieve( ptr ), ptr.clazz );
            }
            catch ( EOFException e )
            {
                logger.error( e.getMessage() );
            }
            catch ( IOException e )
            {
                logger.error( e.getMessage() );
            }
            catch ( ClassNotFoundException e )
            {
                logger.error( e.getMessage() );
            }
            catch ( InstantiationException e )
            {
                logger.error( e.getMessage() );
            }
            catch ( IllegalAccessException e )
            {
                logger.error( e.getMessage() );
            }
        }
        return null;
    }

    public Pointer getPointer( String key )
    {
        return map.get( key );
    }

    public void free( String key )
    {
        Pointer p = map.remove( key );
        if ( p != null )
        {
            memoryManager.free( p );
        }
    }

    public void free( Pointer pointer )
    {
        memoryManager.free( pointer );
    }

    public void collectExpired()
    {
        memoryManager.collectExpired();
        // still have to look for orphan (storing references to freed pointers) map entries
    }

    public void collectLFU()
    {
        memoryManager.collectLFU();
        // can possibly clear one whole buffer if it's too fragmented - investigate
    }

    public void collectAll()
    {
        Thread thread = new Thread()
        {
            public void run()
            {
                logger.info( "begin disposal" );
                collectExpired();
                collectLFU();
                logger.info( "disposal complete" );
            }
        };
        thread.start();
    }


    public void clear()
    {
        map.clear();
        memoryManager.clear();
        logger.info( "Cache cleared" );
    }

    public long entries()
    {
        return map.size();
    }

    public void dump( OffHeapMemoryBuffer mem )
    {
        logger.info( format( "off-heap - buffer: \t%1d", mem.getBufferNumber() ) );
        logger.info( format( "off-heap - allocated: \t%1s", Ram.inMb( mem.capacity() ) ) );
        logger.info( format( "off-heap - used:      \t%1s", Ram.inMb( mem.used() ) ) );
        logger.info( format( "heap 	- max: \t%1s", Ram.inMb( Runtime.getRuntime().maxMemory() ) ) );
        logger.info( format( "heap     - allocated: \t%1s", Ram.inMb( Runtime.getRuntime().totalMemory() ) ) );
        logger.info( format( "heap     - free : \t%1s", Ram.inMb( Runtime.getRuntime().freeMemory() ) ) );
        logger.info( "************************************************" );
    }

    public void dump()
    {
        if ( !logger.isInfoEnabled() )
        {
            return;
        }

        logger.info( "*** DirectMemory statistics ********************" );

        for ( OffHeapMemoryBuffer mem : memoryManager.getBuffers() )
        {
            dump( mem );
        }
    }

    public ConcurrentMap<String, Pointer> getMap()
    {
        return map;
    }

    public void setMap( ConcurrentMap<String, Pointer> map )
    {
        this.map = map;
    }

    public Serializer getSerializer()
    {
        return serializer;
    }

    public void setSerializer( Serializer serializer )
    {
        this.serializer = serializer;
    }

    public MemoryManagerService getMemoryManager()
    {
        return memoryManager;
    }

    public void setMemoryManager( MemoryManagerService memoryManager )
    {
        this.memoryManager = memoryManager;
    }

    @Override
    public Pointer allocate( String key, int size )
    {
        Pointer ptr = memoryManager.allocate( size, -1, -1 );
        map.put( key, ptr );
        ptr.clazz = ByteBuffer.class;
        return ptr;
    }
}
