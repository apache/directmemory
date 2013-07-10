package org.apache.directmemory.guava;

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

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.directmemory.cache.CacheService;

import com.google.common.base.Stopwatch;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheStats;
import com.google.common.cache.ForwardingCache;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import static com.google.common.cache.AbstractCache.SimpleStatsCounter;
import static com.google.common.cache.AbstractCache.StatsCounter;

public class OffHeapCache<K, V>
    extends ForwardingCache.SimpleForwardingCache<K, V>
    implements RemovalListener<K, V>
{
    private final CacheService<K, V> cacheService;

    private final StatsCounter statsCounter = new SimpleStatsCounter();


    public OffHeapCache( CacheService<K, V> cacheService, Cache<K, V> primaryCache, ForwardingListener<K, V> listener )
    {
        super( primaryCache );
        this.cacheService = cacheService;
        listener.setDelegate( this );
    }

    @Override
    public V getIfPresent( Object key )
    {
        V result = super.getIfPresent( key );
        if ( result == null )
        {
            result = retrieve( key );
        }
        return result;
    }


    @Override
    public V get( final K key, final Callable<? extends V> valueLoader )
        throws ExecutionException
    {
        return super.get( key, new Callable<V>()
        {
            @Override
            public V call()
                throws Exception
            {
                //Check in offHeap first
                V result = retrieve( key );

                //Not found in L2 then load
                if ( result == null )
                {
                    result = valueLoader.call();
                }
                return result;
            }
        } );
    }

    @Override
    public ImmutableMap<K, V> getAllPresent( Iterable<?> keys )
    {
        List<?> list = Lists.newArrayList( keys );
        ImmutableMap<K, V> result = super.getAllPresent( list );

        //All the requested keys found then no
        //need to check L2
        if ( result.size() == list.size() )
        {
            return result;
        }

        //Look up value from L2
        Map<K, V> r2 = Maps.newHashMap( result );
        for ( Object key : list )
        {
            if ( !result.containsKey( key ) )
            {
                V val = retrieve( key );
                if ( val != null )
                {
                    //Ideally the signature of method should have been
                    //getAllPresent(Iterable<? extends K> keys) in that
                    //case this cast would not have been required
                    r2.put( (K) key, val );
                }
            }
        }
        return ImmutableMap.copyOf( r2 );
    }

    @Override
    public void invalidate( Object key )
    {
        super.invalidate( key );
        cacheService.free( (K) key );
    }

    @Override
    public void invalidateAll( Iterable<?> keys )
    {
        super.invalidateAll( keys );
        for ( Object key : keys )
        {
            cacheService.free( (K) key );
        }
    }

    /**
     * it invokes clear on MemoryManagerService. If same
     * MemoryManagerService is shared between multiple cacheService
     * then it would lead to clearing of all other caches
     */
    @Override
    public void invalidateAll()
    {
        super.invalidateAll();

        //TODO Problem with calling clear here is that
        cacheService.clear();
    }

    @Override
    public void onRemoval( RemovalNotification<K, V> notification )
    {
        if ( notification.getCause() == RemovalCause.SIZE )
        {
            cacheService.put( notification.getKey(), notification.getValue() );
        }
    }

    public CacheStats offHeapStats()
    {
        return statsCounter.snapshot();
    }

    protected V retrieve( Object key )
    {
        Stopwatch watch = new Stopwatch().start();

        V value = cacheService.retrieve( (K) key );

        if ( value != null )
        {
            statsCounter.recordLoadSuccess( watch.elapsed( TimeUnit.NANOSECONDS ) );
        }
        else
        {
            statsCounter.recordMisses( 1 );
        }

        return value;
    }

}
