package org.apache.directmemory.utils;

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

import java.util.Iterator;

import org.apache.directmemory.cache.CacheService;
import org.apache.directmemory.memory.Pointer;

public class StrictCacheValuesIterator<K, V>
    implements Iterator<V>
{
    private Iterator<K> keysIterator;

    private CacheService<K, V> cacheService;

    private K currentKey;

    public StrictCacheValuesIterator( Iterator<K> keysIterator, CacheService<K, V> cacheService )
    {
        this.keysIterator = keysIterator;
        this.cacheService = cacheService;
    }

    @Override
    public boolean hasNext()
    {
        return keysIterator.hasNext();
    }

    @Override
    public V next()
    {
        currentKey = keysIterator.next();
        Pointer<V> pointer = cacheService.getPointer( currentKey );
        if ( pointer != null && pointer.isExpired() )
        {
            throw new RuntimeException( "Value pointer has expired" );
        }
        return cacheService.retrieve( currentKey );
    }

    @Override
    public void remove()
    {
        cacheService.free( currentKey );
        keysIterator.remove();
    }
};