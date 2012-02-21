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
import org.apache.directmemory.memory.OffHeapMemoryBuffer;
import org.apache.directmemory.memory.Pointer;
import org.apache.directmemory.serialization.Serializer;

import java.util.concurrent.ConcurrentMap;

public interface CacheService
{

    public static int DEFAULT_CONCURRENCY_LEVEL = 4;
    public static int DEFAULT_INITIAL_CAPACITY = 100000;

    void init( int numberOfBuffers, int size, int initialCapacity, int concurrencyLevel );

    void init( int numberOfBuffers, int size );

    void scheduleDisposalEvery( long l );

    Pointer putByteArray( String key, byte[] payload, int expiresIn );

    Pointer putByteArray( String key, byte[] payload );

    Pointer put( String key, Object object );

    Pointer put( String key, Object object, int expiresIn );

    byte[] retrieveByteArray( String key );

    Object retrieve( String key );

    Pointer getPointer( String key );

    void free( String key );

    void free( Pointer pointer );

    void collectExpired();

    void collectLFU();

    void collectAll();


    void clear();

    long entries();

    void dump( OffHeapMemoryBuffer mem );

    void dump();

    ConcurrentMap<String, Pointer> getMap();

    void setMap( ConcurrentMap<String, Pointer> map );

    Serializer getSerializer();

    MemoryManagerService getMemoryManager();

    void setMemoryManager( MemoryManagerService memoryManager );

    Pointer allocate( String key, int size );

}
