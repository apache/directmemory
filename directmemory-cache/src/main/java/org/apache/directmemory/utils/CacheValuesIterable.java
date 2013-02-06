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

/**
 * A simple {@link Iterable} over {@link CacheService}'s values
 */
public class CacheValuesIterable<K, V>
    implements Iterable<V>
{

    private final CacheService<K, V> cacheService;

    private final Iterator<K> keysIterator;

    public CacheValuesIterable( CacheService<K, V> cacheService )
    {
        this.cacheService = cacheService;
        this.keysIterator = cacheService.getMap().keySet().iterator();
    }

    @Override
    public Iterator<V> iterator()
    {
        return new Iterator<V>()
        {
            @Override
            public boolean hasNext()
            {
                return keysIterator.hasNext();
            }

            @Override
            public V next()
            {
                K nextKey = keysIterator.next();
                Pointer<V> pointer = cacheService.getPointer( nextKey );
                if ( pointer != null && pointer.isExpired() )
                {
                    throw new RuntimeException( "Value pointer has expired" );
                }
                return cacheService.retrieve( nextKey );
            }

            @Override
            public void remove()
            {
                keysIterator.remove();
            }
        };
    }
}
