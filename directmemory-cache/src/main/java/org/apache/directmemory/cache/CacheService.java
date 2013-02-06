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

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.directmemory.memory.MemoryManagerService;
import org.apache.directmemory.memory.Pointer;
import org.apache.directmemory.serialization.Serializer;
import org.slf4j.Logger;

public interface CacheService<K, V>
    extends Closeable
{

    /**
     * Schedules the disposal event with the given period in milliseconds.
     * 
     * @param period The time period in milliseconds
     */
    void scheduleDisposalEvery( long period );

    /**
     * Schedules the disposal event with the given period with the given {@link TimeUnit}.
     * 
     * @param period The time period
     * @param unit The period's timeunit
     */
    void scheduleDisposalEvery( long period, TimeUnit unit );

    /**
     * Stored a preserialized payload with the given key. The expiration is set to the given expiresIn value in
     * milliseconds. If not enough space found to store the payload the returned pointer is null.
     * 
     * @param key The key to save the payload with
     * @param payload The preserialized payload as bytearray
     * @param expiresIn The expiration delay
     * @return The created pointer to directly retrieve the payload or null if not enough space was found
     */
    Pointer<V> putByteArray( K key, byte[] payload, long expiresIn );

    /**
     * Stored a preserialized payload with the given key with no expiration value. If not enough space found to store
     * the payload the returned pointer is null.
     * 
     * @param key The key to save the payload with
     * @param payload The preserialized payload as bytearray
     * @return The created pointer to directly retrieve the payload or null if not enough space was found
     */
    Pointer<V> putByteArray( K key, byte[] payload );

    /**
     * Serializes and stored the given value using the key and sets the expiresIn value for the expiration of the key.
     * If not enough space found to store the payload the returned pointer is null.
     * 
     * @param key The key to save the value with
     * @param value The value to serialize and store
     * @param expiresIn The expiration delay
     * @return The created pointer to directly retrieve the payload or null if not enough space was found
     */
    Pointer<V> put( K key, V value, int expiresIn );

    /**
     * Serializes and stored the given value using the key with no expiration value. If not enough space found to store
     * the payload the returned pointer is null.
     * 
     * @param key The key to save the value with
     * @param value The value to serialize and store
     * @return The created pointer to directly retrieve the payload or null if not enough space was found
     */
    Pointer<V> put( K key, V value );

    /**
     * Retrieves the stored payload for key as a bytearray. If no pointer is found for the given key null is returned.
     * 
     * @param key The key to retrieve
     * @return The payload as bytearray or null if key was not found
     */
    byte[] retrieveByteArray( K key );

    /**
     * Retrieves the stored, deserialized value for key. If no pointer is found for the given key null is returned.
     * 
     * @param key The key to retrieve
     * @return The deserialized value or null if key was not found
     */
    V retrieve( K key );

    /**
     * Retrieves the accociated {@link Pointer} to the given key or null if no pointer was found.
     * 
     * @param key The key to retrieve
     * @return The pointer of the key or null if key was not found
     */
    Pointer<V> getPointer( K key );

    /**
     * Removes the key and frees the underlying memory area.
     * 
     * @param key The key to remove
     */
    void free( K key );

    /**
     * Removes the pointer and frees the underlying memory area.
     * 
     * @param pointer The pointer to remove
     */
    void free( Pointer<V> pointer );

    /**
     * Tells the {@link CacheService} to collect and remove all expired keys. In most cases this is automatically
     * handled by scheduling a disposal interval using {@link CacheService#scheduleDisposalEvery} and there are very
     * rare cases where this needs to be called manually.
     */
    void collectExpired();

    /**
     * Tells the {@link CacheService} to collect and remove all least frequently used keys. This operation could
     * possibly clear the whole cache if there were no recent actions. In most cases this is automatically handled by
     * scheduling a disposal interval using {@link CacheService#scheduleDisposalEvery} and there are very rare cases
     * where this needs to be called manually.
     */
    void collectLFU();

    /**
     * Tells the {@link CacheService} to collect and remove all expired AND least frequently used keys. In most cases
     * this is automatically handled by scheduling a disposal interval using {@link CacheService#scheduleDisposalEvery}
     * and there are very rare cases where this needs to be called manually.
     */
    void collectAll();

    /**
     * Clears the whole cache by removing all stored keys. It is up to the underlying {@link MemoryManagerService}
     * implementation to free allocated memory or not.
     */
    void clear();

    /**
     * Retrieves the count of the current entries.
     * 
     * @return Number of entries
     */
    long entries();

    /**
     * Dumps information about the actual internal {@link MemoryManagerService} to the configured {@link Logger} with
     * info loglevel.
     */
    void dump();

    /**
     * Retrieves a map of all available keys and their according {@link Pointer}s. It is up to the {@link CacheService}
     * implementation if the retrieved map is threadsafe or not. The standard implementation uses a
     * {@link ConcurrentHashMap}.
     * 
     * @return A mapping of keys to their according pointers
     */
    Map<K, Pointer<V>> getMap();

    /**
     * Retrieves the internally used {@link Serializer} implementation.
     * 
     * @return The used serializer
     */
    Serializer getSerializer();

    /**
     * Retrieves the internally used {@link MemoryManagerService} implementation.
     * 
     * @return The used memory manager
     */
    MemoryManagerService<V> getMemoryManager();

    /**
     * Explicitly allocated a bunch of bytes in the cache using a given key and type and returns the created
     * {@link Pointer}.
     * 
     * @param key The key to store as
     * @param type The datatype of the underlying data
     * @param size The size to allocate for this pointer
     * @return
     */
    <T extends V> Pointer<V> allocate( K key, Class<T> type, int size );

}
