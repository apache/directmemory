package org.apache.directmemory.ehcache;

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

import org.apache.directmemory.cache.CacheService;
import org.apache.directmemory.cache.CacheServiceImpl;
import org.apache.directmemory.memory.MemoryManagerService;
import org.apache.directmemory.memory.MemoryManagerServiceWithAllocationPolicyImpl;
import org.apache.directmemory.memory.OffHeapMemoryBuffer;
import org.apache.directmemory.memory.Pointer;
import org.apache.directmemory.memory.RoundRobinAllocationPolicy;
import org.apache.directmemory.serialization.Serializer;

import java.util.Set;

/**
 * @param <K>
 * @param <V>
 * @author michaelandrepearce
 */
public class DirectMemoryCache<K, V>
{

    private MemoryManagerService<V> memoryManager =
        new MemoryManagerServiceWithAllocationPolicyImpl<V>( new RoundRobinAllocationPolicy<V>(), false );

    private CacheServiceImpl<K, V> cacheService = new CacheServiceImpl<K, V>( memoryManager );

    public DirectMemoryCache( int numberOfBuffers, int size, int initialCapacity, int concurrencyLevel )
    {
        cacheService.init( numberOfBuffers, size, initialCapacity, concurrencyLevel );
    }

    public DirectMemoryCache( int numberOfBuffers, int size )
    {
        this( numberOfBuffers, size, CacheService.DEFAULT_INITIAL_CAPACITY, CacheService.DEFAULT_CONCURRENCY_LEVEL );
    }

    public void scheduleDisposalEvery( long l )
    {
        cacheService.scheduleDisposalEvery( l );
    }

    public Pointer<V> putByteArray( K key, byte[] payload, int expiresIn )
    {
        return cacheService.putByteArray( key, payload, expiresIn );
    }

    public Pointer<V> putByteArray( K key, byte[] payload )
    {
        return cacheService.putByteArray( key, payload );
    }

    public Pointer<V> put( K key, V value )
    {
        return cacheService.put( key, value );
    }

    public Pointer<V> put( K key, V value, int expiresIn )
    {
        return cacheService.put( key, value, expiresIn );
    }

    public byte[] retrieveByteArray( K key )
    {
        return cacheService.retrieveByteArray( key );
    }

    public V retrieve( K key )
    {
        return cacheService.retrieve( key );
    }

    public Pointer<V> getPointer( K key )
    {
        return cacheService.getPointer( key );
    }

    public void free( K key )
    {
        cacheService.free( key );
    }

    public void free( Pointer<V> pointer )
    {
        cacheService.free( pointer );
    }

    public void collectExpired()
    {
        cacheService.collectExpired();
    }

    public void collectLFU()
    {
        cacheService.collectLFU();
    }

    public void collectAll()
    {
        cacheService.collectAll();
    }

    public void clear()
    {
        cacheService.clear();
    }

    public long size()
    {
        return cacheService.entries();
    }

    public long sizeInBytes()
    {
        long i = 0;
        for ( OffHeapMemoryBuffer<V> buffer : getMemoryManager().getBuffers() )
        {
            i += buffer.used();
        }
        return i;
    }

    public long capacityInBytes()
    {
        long i = 0;
        for ( OffHeapMemoryBuffer<V> buffer : getMemoryManager().getBuffers() )
        {
            i += buffer.capacity();
        }
        return i;
    }

    public void dump( OffHeapMemoryBuffer<V> mem )
    {
        cacheService.dump( mem );
    }

    public void dump()
    {
        cacheService.dump();
    }

    public Serializer getSerializer()
    {
        return cacheService.getSerializer();
    }

    public MemoryManagerService<V> getMemoryManager()
    {
        return cacheService.getMemoryManager();
    }

    public Pointer<V> allocate( K key, Class<V> type, int size )
    {
        return cacheService.allocate( key, type, size );
    }

    public Set<K> getKeys()
    {
        return cacheService.getMap().keySet();
    }

    public boolean containsKey( K key )
    {
        return cacheService.getMap().containsKey( key );
    }

}
