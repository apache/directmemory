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

import java.util.Date;

@Deprecated
public interface OffHeapMemoryBuffer<T>
{

    /**
     * @return the current usage of the buffer
     */
    public int used();

    /**
     * @return the capacity of the buffer
     */
    public int capacity();

    /**
     * @return buffer identity number
     */
    public int getBufferNumber();

    /**
     * Store given payload in the buffer, returning the pointer on success, null is the storing failed (mostly buffer full)
     * @param payload : the data to store
     * @return the pointer where the data is stored, null in case of failure
     */
    public Pointer<T> store( byte[] payload );

    /**
     * Same as {@link #store(byte[])}, with an absolute expiration date
     * @param payload : the data to store
     * @param expires : an absolute expiration date
     * @return the pointer where the data is stored, null in case of failure
     */
    public Pointer<T> store( byte[] payload, Date expires );

    /**
     * Same as {@link #store(byte[])}, with an relative expiration delta
     * @param payload : the data to store
     * @param expiresIn : a relative expiration delta
     * @return the pointer where the data is stored, null in case of failure
     */
    public Pointer<T> store( byte[] payload, long expiresIn );

    /**
     * Return previously stored data associated to the given pointer
     * @param pointer : presiously allocated pointer
     * @return previously stored data
     */
    public byte[] retrieve( Pointer<T> pointer );

    /**
     * Release previously allocated memory
     * @param pointer2free : the pointer to free
     * @return the newly freed space
     */
    public int free( Pointer<T> pointer2free );

    /**
     * Completely empty the buffer
     */
    public void clear();

    /**
     * TODO
     */
    public void disposeExpiredRelative();

    /**
     * TODO
     */
    public void disposeExpiredAbsolute();

    /**
     * Iterate through the buffer and free data that have expired
     * @return newly freed space
     */
    public long collectExpired();

    /**
     * Free the least recently used data
     * @param limit : number of pointers to free. Setting a negative value will free 10% of the objects
     * @return the amount of freed space
     */
    public long collectLFU( int limit );

    /**
     * Update the data at a given pointer
     * @param pointer
     * @param payload : the data to update
     * @return the update pointer. It may be a new pointer.
     */
    public Pointer<T> update( Pointer<T> pointer, byte[] payload );

    /**
     * Allocate requested size and return a pointer and a ByteBuffer
     *
     * @param size
     * @param expiresIn
     * @param expires
     * @return
     */
    public <V extends T> Pointer<T> allocate( Class<V> type, int size, long expiresIn, long expires );
}
