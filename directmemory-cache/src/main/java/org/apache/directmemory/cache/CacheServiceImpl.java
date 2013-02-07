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

import org.apache.directmemory.measures.Ram;
import org.apache.directmemory.memory.MemoryManagerService;
import org.apache.directmemory.memory.Pointer;
import org.apache.directmemory.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

public class CacheServiceImpl<K, V>
    implements MutableCacheService<K, V>
{

    private static final Logger logger = LoggerFactory.getLogger( CacheServiceImpl.class );

    private ConcurrentMap<K, Pointer<V>> map;

    private Serializer serializer;

    private MemoryManagerService<V> memoryManager;

    private final Timer timer = new Timer(true);

    /**
     * Constructor
     */
    public CacheServiceImpl( ConcurrentMap<K, Pointer<V>> map, MemoryManagerService<V> memoryManager,
                             Serializer serializer )
    {
        checkArgument( map != null, "Impossible to initialize the CacheService with a null map" );
        checkArgument( memoryManager != null, "Impossible to initialize the CacheService with a null memoryManager" );
        checkArgument( serializer != null, "Impossible to initialize the CacheService with a null serializer" );

        this.map = map;
        this.memoryManager = memoryManager;
        this.serializer = serializer;
    }

    @Override
    public void scheduleDisposalEvery( long period, TimeUnit unit )
    {
        scheduleDisposalEvery( unit.toMillis( period ) );
    }
    
    @Override
    public void scheduleDisposalEvery( long period )
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
        }, period, period );

        logger.info( "disposal scheduled every {} milliseconds", period );
    }

    @Override
    public Pointer<V> putByteArray( K key, byte[] payload )
    {
        return store( key, payload, 0 );
    }

    @Override
    public Pointer<V> putByteArray( K key, byte[] payload, long expiresIn )
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
            if ( ptr != null )
            {
                @SuppressWarnings( "unchecked" ) // type driven by the compiler
                    Class<? extends V> clazz = (Class<? extends V>) value.getClass();

                ptr.setClazz( clazz );
            }
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

    private Pointer<V> store( K key, byte[] payload, long expiresIn )
    {
        Pointer<V> pointer = map.get( key );
        if ( pointer != null )
        {
        	memoryManager.free( pointer );
        }
        pointer = memoryManager.store( payload, expiresIn );
        if ( pointer != null )
        {
            map.put( key, pointer );
        }
        return pointer;
    }

    @Override
    public byte[] retrieveByteArray( K key )
    {
        Pointer<V> ptr = getPointer( key );
        if ( ptr == null )
        {
            return null;
        }
        if ( ptr.isExpired() || ptr.isFree() )
        {
            map.remove( key );
            if ( !ptr.isFree() )
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
        if ( ptr.isExpired() || ptr.isFree() )
        {
            map.remove( key );
            if ( !ptr.isFree() )
            {
                memoryManager.free( ptr );
            }
            return null;
        }
        else
        {
            try
            {
                return serializer.deserialize( memoryManager.retrieve( ptr ), ptr.getClazz() );
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
    public void close()
        throws IOException
    {
        memoryManager.close();
        logger.info( "Cache closed" );
    }

    @Override
    public long entries()
    {
        return map.size();
    }

    public void dump( MemoryManagerService<V> mms )
    {
        logger.info( format( "off-heap - allocated: \t%1s", Ram.inMb( mms.capacity() ) ) );
        logger.info( format( "off-heap - used:      \t%1s", Ram.inMb( mms.used() ) ) );
        logger.info( format( "heap  - max: \t%1s", Ram.inMb( Runtime.getRuntime().maxMemory() ) ) );
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

        dump( memoryManager );
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
        ptr.setClazz( type );
        return ptr;
    }
}
