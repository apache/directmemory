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


import org.apache.directmemory.memory.MemoryManagerService;
import org.apache.directmemory.memory.Pointer;
import org.apache.directmemory.serialization.Serializer;

import java.util.concurrent.ConcurrentMap;

public interface CacheService<K, V>
{

    void scheduleDisposalEvery( long l );

    Pointer<V> putByteArray( K key, byte[] payload, int expiresIn );

    Pointer<V> putByteArray( K key, byte[] payload );

    Pointer<V> put( K key, V value );

    Pointer<V> put( K key, V value, int expiresIn );

    byte[] retrieveByteArray( K key );

    V retrieve( K key );

    Pointer<V> getPointer( K key );

    void free( K key );

    void free( Pointer<V> pointer );

    void collectExpired();

    void collectLFU();

    void collectAll();


    void clear();

    long entries();

    void dump();

    ConcurrentMap<K, Pointer<V>> getMap();

    void setMap( ConcurrentMap<K, Pointer<V>> map );

    Serializer getSerializer();

    MemoryManagerService<V> getMemoryManager();

    void setMemoryManager( MemoryManagerService<V> memoryManager );

    void setSerializer( Serializer serializer );

    <T extends V> Pointer<V> allocate( K key, Class<T> type, int size );

}
