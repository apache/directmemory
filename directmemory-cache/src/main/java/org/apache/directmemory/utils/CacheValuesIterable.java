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

/**
 * A simple {@link Iterable} over {@link CacheService}'s values
 */
public class CacheValuesIterable<K, V>
    implements Iterable<V>
{

    private final CacheService<K, V> cacheService;

    private final Iterator<K> keysIterator;

    private final boolean strict;

    /**
     * Creates a {@link StrictCacheValuesIterator} over cache values.
     * 
     * @param cacheService
     */
    public CacheValuesIterable( CacheService<K, V> cacheService )
    {
        this( cacheService, true );
    }

    /**
     * Creates a iterator over cache values.
     * 
     * @param cacheService the {@link CacheService} on whose values this will iterate
     * @param strict When <code>true</code> the resulting iterator might returns expired or stalled values. Please see
     *            {@link NonStrictCacheValuesIterator} for details
     */
    public CacheValuesIterable( CacheService<K, V> cacheService, boolean strict )
    {
        this.cacheService = cacheService;
        this.keysIterator = cacheService.getMap().keySet().iterator();
        this.strict = strict;
    }

    @Override
    public Iterator<V> iterator()
    {
        return strict ? new StrictCacheValuesIterator<K, V>( keysIterator, cacheService )
                        : new NonStrictCacheValuesIterator<K, V>( keysIterator, cacheService );
    }
}
