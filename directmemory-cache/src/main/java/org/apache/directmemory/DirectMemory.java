package org.apache.directmemory;

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

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static org.apache.directmemory.measures.In.seconds;
import static org.apache.directmemory.serialization.SerializerFactory.createNewSerializer;

import java.util.concurrent.ConcurrentMap;

import org.apache.directmemory.cache.CacheService;
import org.apache.directmemory.cache.CacheServiceImpl;
import org.apache.directmemory.measures.Ram;
import org.apache.directmemory.memory.MemoryManagerService;
import org.apache.directmemory.memory.MemoryManagerServiceImpl;
import org.apache.directmemory.memory.Pointer;
import org.apache.directmemory.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.MapMaker;

public final class DirectMemory<K, V>
{

    public static int DEFAULT_CONCURRENCY_LEVEL = 4;

    public static int DEFAULT_INITIAL_CAPACITY = 100000;

    public static int DEFAULT_DISPOSAL_TIME = 10; // seconds

    private final Logger logger = LoggerFactory.getLogger( getClass() );

    private int numberOfBuffers;

    private int size;

    private int initialCapacity = DEFAULT_INITIAL_CAPACITY;

    private int concurrencyLevel = DEFAULT_CONCURRENCY_LEVEL;

    private long disposalTime = seconds( DEFAULT_DISPOSAL_TIME );

    private ConcurrentMap<K, Pointer<V>> map;

    private Serializer serializer;

    private MemoryManagerService<V> memoryManager;

    public DirectMemory()
    {
        // does nothing
    }

    public DirectMemory( DirectMemory<K, V> prototype )
    {
        checkArgument( prototype != null, "Impossible to create a DirectMemory instance from a null prototype" );

        numberOfBuffers = prototype.numberOfBuffers;
        size = prototype.size;
        initialCapacity = prototype.initialCapacity;
        concurrencyLevel = prototype.concurrencyLevel;
        disposalTime = prototype.disposalTime;

        map = prototype.map;
        serializer = prototype.serializer;
        memoryManager = prototype.memoryManager;
    }

    public DirectMemory<K, V> setNumberOfBuffers( int numberOfBuffers )
    {
        checkArgument( numberOfBuffers > 0, "Impossible to create a CacheService with a number of buffers lesser than 1" );
        this.numberOfBuffers = numberOfBuffers;
        return this;
    }

    public DirectMemory<K, V> setSize( int size )
    {
        checkArgument( size > 0, "Impossible to create a CacheService with a size lesser than 1" );
        this.size = size;
        return this;
    }

    public DirectMemory<K, V> setInitialCapacity( int initialCapacity )
    {
        checkArgument( initialCapacity > 0, "Impossible to create a CacheService with an initialCapacity lesser than 1" );
        this.initialCapacity = initialCapacity;
        return this;
    }

    public DirectMemory<K, V> setConcurrencyLevel( int concurrencyLevel )
    {
        checkArgument( concurrencyLevel > 0, "Impossible to create a CacheService with a concurrencyLevel lesser than 1" );
        this.concurrencyLevel = concurrencyLevel;
        return this;
    }

    public DirectMemory<K, V> setDisposalTime( long disposalTime )
    {
        checkArgument( disposalTime > 0, "Impossible to create a CacheService with a disposalTime lesser than 1" );
        this.disposalTime = disposalTime;
        return this;
    }

    public DirectMemory<K, V> setMap( ConcurrentMap<K, Pointer<V>> map )
    {
        checkArgument( map != null, "Impossible to create a CacheService with a null map" );
        this.map = map;
        return this;
    }

    public DirectMemory<K, V> setSerializer( Serializer serializer )
    {
        checkArgument( serializer != null, "Impossible to create a CacheService with a null serializer" );
        this.serializer = serializer;
        return this;
    }

    public DirectMemory<K, V> setMemoryManager( MemoryManagerService<V> memoryManager )
    {
        checkArgument( memoryManager != null, "Impossible to create a CacheService with a null memoryManager" );
        this.memoryManager = memoryManager;
        return this;
    }

    public CacheService<K, V> newCacheService()
    {
        if ( map == null )
        {
            map = new MapMaker().concurrencyLevel( concurrencyLevel ).initialCapacity( initialCapacity ).makeMap();
        }
        if ( memoryManager == null )
        {
            memoryManager = new MemoryManagerServiceImpl<V>();
        }
        if ( serializer == null )
        {
            serializer = createNewSerializer();
        }

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

        CacheService<K, V> cacheService = new CacheServiceImpl<K, V>( map, memoryManager, serializer );
        cacheService.scheduleDisposalEvery( disposalTime );
        return cacheService;
    }

}
