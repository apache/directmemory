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

import static java.lang.String.format;
import static org.apache.directmemory.serialization.SerializerFactory.createNewSerializer;

import java.io.EOFException;
import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentMap;

import org.apache.directmemory.measures.Every;
import org.apache.directmemory.measures.Ram;
import org.apache.directmemory.memory.MemoryManagerService;
import org.apache.directmemory.memory.MemoryManagerServiceImpl;
import org.apache.directmemory.memory.OffHeapMemoryBuffer;
import org.apache.directmemory.memory.Pointer;
import org.apache.directmemory.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.MapMaker;

public class CacheServiceImpl<K, V>
    implements CacheService<K, V>
{

    private static Logger logger = LoggerFactory.getLogger( CacheServiceImpl.class );

    private ConcurrentMap<K, Pointer<V>> map;

    private Serializer serializer = createNewSerializer();

    private MemoryManagerService<V> memoryManager = new MemoryManagerServiceImpl<V>();

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
    public CacheServiceImpl( MemoryManagerService<V> memoryManager )
    {
        this.memoryManager = memoryManager;
    }

    @Override
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

    @Override
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

    @Override
    public void init( int numberOfBuffers, int size )
    {
        init( numberOfBuffers, size, DEFAULT_INITIAL_CAPACITY, DEFAULT_CONCURRENCY_LEVEL );
    }

    @Override
    public Pointer<V> putByteArray( K key, byte[] payload )
    {
        return store( key, payload, 0 );
    }

    @Override
    public Pointer<V> putByteArray( K key, byte[] payload, int expiresIn )
    {
        return store( key, payload, expiresIn );
    }

    @Override
    public Pointer<V> put( K key, V value )
    {
        return put( key, value, 0 );
    }

    @Override
    public Pointer<V> put( K key, V value, int expiresIn )
    {
        try
        {
            byte[] payload = serializer.serialize( value );
            Pointer<V> ptr = store( key, payload, expiresIn );

            @SuppressWarnings( "unchecked" ) // type driven by the compiler
            Class<? extends V> clazz = (Class<? extends V>) value.getClass();

            ptr.clazz = clazz;
            return ptr;
        }
        catch ( IOException e )
        {

            if ( logger.isDebugEnabled() )
            {
                logger.debug( "IOException put object in cache:{}", e.getMessage(), e );
            }
            else
            {
                logger.error( "IOException put object in cache:{}", e.getMessage() );
            }
            return null;
        }
    }

    private Pointer<V> store( K key, byte[] payload, int expiresIn )
    {
        Pointer<V> pointer = map.get( key );
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

    @Override
    public byte[] retrieveByteArray( K key )
    {
        Pointer<V> ptr = getPointer( key );
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

    @Override
    public V retrieve( K key )
    {
        Pointer<V> ptr = getPointer( key );
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

    @Override
    public Pointer<V> getPointer( K key )
    {
        return map.get( key );
    }

    @Override
    public void free( K key )
    {
        Pointer<V> p = map.remove( key );
        if ( p != null )
        {
            memoryManager.free( p );
        }
    }

    @Override
    public void free( Pointer<V> pointer )
    {
        memoryManager.free( pointer );
    }

    @Override
    public void collectExpired()
    {
        memoryManager.collectExpired();
        // still have to look for orphan (storing references to freed pointers) map entries
    }

    @Override
    public void collectLFU()
    {
        memoryManager.collectLFU();
        // can possibly clear one whole buffer if it's too fragmented - investigate
    }

    @Override
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


    @Override
    public void clear()
    {
        map.clear();
        memoryManager.clear();
        logger.info( "Cache cleared" );
    }

    @Override
    public long entries()
    {
        return map.size();
    }

    @Override
    public void dump( OffHeapMemoryBuffer<V> mem )
    {
        logger.info( format( "off-heap - buffer: \t%1d", mem.getBufferNumber() ) );
        logger.info( format( "off-heap - allocated: \t%1s", Ram.inMb( mem.capacity() ) ) );
        logger.info( format( "off-heap - used:      \t%1s", Ram.inMb( mem.used() ) ) );
        logger.info( format( "heap 	- max: \t%1s", Ram.inMb( Runtime.getRuntime().maxMemory() ) ) );
        logger.info( format( "heap     - allocated: \t%1s", Ram.inMb( Runtime.getRuntime().totalMemory() ) ) );
        logger.info( format( "heap     - free : \t%1s", Ram.inMb( Runtime.getRuntime().freeMemory() ) ) );
        logger.info( "************************************************" );
    }

    @Override
    public void dump()
    {
        if ( !logger.isInfoEnabled() )
        {
            return;
        }

        logger.info( "*** DirectMemory statistics ********************" );

        for ( OffHeapMemoryBuffer<V> mem : memoryManager.getBuffers() )
        {
            dump( mem );
        }
    }

    @Override
    public ConcurrentMap<K, Pointer<V>> getMap()
    {
        return map;
    }

    @Override
    public void setMap( ConcurrentMap<K, Pointer<V>> map )
    {
        this.map = map;
    }

    @Override
    public Serializer getSerializer()
    {
        return serializer;
    }

    @Override
    public void setSerializer( Serializer serializer )
    {
        this.serializer = serializer;
    }

    @Override
    public MemoryManagerService<V> getMemoryManager()
    {
        return memoryManager;
    }

    @Override
    public void setMemoryManager( MemoryManagerService<V> memoryManager )
    {
        this.memoryManager = memoryManager;
    }

    @Override
    public <T extends V> Pointer<V> allocate( K key, Class<T> type, int size )
    {
        Pointer<V> ptr = memoryManager.allocate( type, size, -1, -1 );
        map.put( key, ptr );
        ptr.clazz = type;
        return ptr;
    }
}
