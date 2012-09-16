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
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import org.apache.directmemory.DirectMemory;
import org.apache.directmemory.memory.MemoryManagerService;
import org.apache.directmemory.memory.Pointer;
import org.apache.directmemory.serialization.Serializer;

public class Cache
{

    private static final DirectMemory<String, Object> builder = new DirectMemory<String, Object>();

    private static CacheService<String, Object> cacheService = builder.newCacheService();

    // olamy chicken and eggs isssue
    // private static CacheService cacheService = new CacheServiceImpl( getMemoryManager());

    private Cache()
    {
        // not instantiable
    }

    public static void scheduleDisposalEvery( long l )
    {
        // store to builder
        builder.setDisposalTime( l );

        cacheService.scheduleDisposalEvery( l );
    }

    public static void init( int numberOfBuffers, int size, int initialCapacity, int concurrencyLevel )
    {
        cacheService =
            builder.setNumberOfBuffers( numberOfBuffers ).setInitialCapacity( initialCapacity )
                .setConcurrencyLevel(concurrencyLevel )
                    .setSize( size ).newCacheService();
//        concurrencyLevel ).setMemoryManager( new UnsafeMemoryManagerServiceImpl<Object>() ).setSize( size ).newCacheService();
    }

    public static void init( int numberOfBuffers, int size )
    {
        init( numberOfBuffers, size, DirectMemory.DEFAULT_INITIAL_CAPACITY, DirectMemory.DEFAULT_CONCURRENCY_LEVEL );
    }

    public static Pointer<Object> putByteArray( String key, byte[] payload, int expiresIn )
    {
        return cacheService.putByteArray( key, payload, expiresIn );
    }

    public static Pointer<Object> putByteArray( String key, byte[] payload )
    {
        return cacheService.putByteArray( key, payload );
    }

    public static Pointer<Object> put( String key, Object object )
    {
        return cacheService.put( key, object );
    }

    public static Pointer<Object> put( String key, Object object, int expiresIn )
    {
        return cacheService.put( key, object, expiresIn );
    }

    public static byte[] retrieveByteArray( String key )
    {
        return cacheService.retrieveByteArray( key );
    }

    public static Object retrieve( String key )
    {
        return cacheService.retrieve( key );
    }

    public static Pointer<Object> getPointer( String key )
    {
        return cacheService.getPointer( key );
    }

    public static void free( String key )
    {
        cacheService.free( key );
    }

    public static void free( Pointer<Object> pointer )
    {
        cacheService.free( pointer );
    }

    public static void collectExpired()
    {
        cacheService.collectExpired();
    }

    public static void collectLFU()
    {
        cacheService.collectLFU();
    }

    public static void collectAll()
    {
        cacheService.collectAll();
    }


    public static void clear()
    {
        cacheService.clear();
    }

    public static long entries()
    {
        return cacheService.entries();
    }

    public static void dump()
    {
        cacheService.dump();
    }

    public static Serializer getSerializer()
    {
        return cacheService.getSerializer();
    }

    public static MemoryManagerService<Object> getMemoryManager()
    {
        return cacheService.getMemoryManager();
    }

    public static Pointer<Object> allocate( String key, int size )
    {
        return cacheService.allocate( key, Object.class, size );
    }

}
