package org.apache.directmemory.memory;

import java.io.Closeable;
import java.util.Set;

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

public interface MemoryManagerService<V>
    extends Closeable
{

    /**
     * Initialize the internal structure. Need to be called before the service can be used.
     * 
     * @param numberOfBuffers : number of internal bucket
     * @param size : size in B of internal buckets
     */
    void init( int numberOfBuffers, int size );

    /**
     * Store function family. Store the given payload at a certain offset in a MemoryBuffer, returning the pointer to
     * the value.
     * 
     * @param payload : the data to store
     * @return the pointer to the value, or null if not enough space has been found.
     */
    Pointer<V> store( byte[] payload, long expiresIn );

    /**
     * Same function as {@link #store(byte[])}, but add an relative expiration delta in milliseconds
     * 
     * @param payload : the data to store
     * @param expiresIn : relative amount of milliseconds the data will expire
     * @return the pointer to the value, or null if not enough space has been found.
     */
    Pointer<V> store( byte[] payload );

    /**
     * Same function as {@link #store(byte[])}, but add an absolute expiration date
     * 
     * @param payload : the data to store
     * @param expires : the absolute date the data will expire
     * @return the pointer to the value, or null if not enough space has been found.
     */
    // public Pointer store(byte[] payload, Date expires);

/**
     *
     *
     * Update value of a {@link Pointer
     * @param pointer
     * @param payload
     * @return
     * @throw BufferOverflowException if the size of the payload id bigger than the pointer capacity
     */
    Pointer<V> update( Pointer<V> pointer, byte[] payload );

    byte[] retrieve( Pointer<V> pointer );

    Pointer<V> free( Pointer<V> pointer );

    void clear();

    long capacity();

    long used();

    long collectExpired();

    void collectLFU();

    <T extends V> Pointer<V> allocate( Class<T> type, int size, long expiresIn, long expires );

    Set<Pointer<V>> getPointers();

}
