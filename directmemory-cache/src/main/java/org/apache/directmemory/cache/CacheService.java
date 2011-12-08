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

package org.apache.directmemory.cache;

import java.util.concurrent.ConcurrentMap;
import org.apache.directmemory.memory.MemoryManagerService;
import org.apache.directmemory.memory.OffHeapMemoryBuffer;
import org.apache.directmemory.memory.Pointer;
import org.apache.directmemory.serialization.Serializer;

public interface CacheService {

  public static int DEFAULT_CONCURRENCY_LEVEL = 4;
  public static int DEFAULT_INITIAL_CAPACITY = 100000;

  public void init(int numberOfBuffers, int size, int initialCapacity, int concurrencyLevel);

  public void init(int numberOfBuffers, int size);

  public void scheduleDisposalEvery(long l);

  public Pointer putByteArray(String key, byte[] payload, int expiresIn);

  public Pointer putByteArray(String key, byte[] payload);

  public Pointer put(String key, Object object);

  public Pointer put(String key, Object object, int expiresIn);

  public Pointer updateByteArray(String key, byte[] payload);

  public Pointer update(String key, Object object);

  public byte[] retrieveByteArray(String key);

  public Object retrieve(String key);

  public Pointer getPointer(String key);

  public void free(String key);

  public void free(Pointer pointer);

  public void collectExpired();

  public void collectLFU();

  public void collectAll();


  public void clear();

  public long entries();

  public void dump(OffHeapMemoryBuffer mem);

  public void dump();

  public ConcurrentMap<String, Pointer> getMap();

  public void setMap(ConcurrentMap<String, Pointer> map);

  public Serializer getSerializer();

  public MemoryManagerService getMemoryManager();

  public void setMemoryManager(MemoryManagerService memoryManager);

  public Pointer allocate(String key, int size);

}
