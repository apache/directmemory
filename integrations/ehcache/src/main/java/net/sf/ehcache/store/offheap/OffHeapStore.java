package net.sf.ehcache.store.offheap;

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

import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Ehcache;
import net.sf.ehcache.config.CacheConfiguration;
import net.sf.ehcache.pool.Pool;
import net.sf.ehcache.pool.PoolableStore;
import net.sf.ehcache.pool.impl.UnboundedPool;
import net.sf.ehcache.store.FrontEndCacheTier;
import net.sf.ehcache.store.MemoryStore;
import net.sf.ehcache.store.Store;
import net.sf.ehcache.store.disk.DiskStore;
import org.apache.directmemory.ehcache.DirectMemoryStore;

import java.util.WeakHashMap;

/**
 * This class is simply a connector class into the EHCache for OffHeap.
 *
 * @author michaelandrepearce
 */
public class OffHeapStore
{

    private static final WeakHashMap<CacheManager, Pool<PoolableStore>> OFFHEAP_POOLS =
        new WeakHashMap<CacheManager, Pool<PoolableStore>>();

    public static Store create( Ehcache cache, String diskStorePath, Pool<PoolableStore> onHeapPool,
                                Pool<PoolableStore> onDiskPool )
    {

        CacheConfiguration config = cache.getCacheConfiguration();
        MemoryStore memoryStore = createMemoryStore( cache, onHeapPool );
        DirectMemoryStore offHeapStore = createOffHeapStore( cache, true );
        DiskStore diskStore = null; //need to implement disk backing to store.
        Store store = null;
        if ( diskStore == null )
        {
            store = new FrontEndCacheTier<MemoryStore, DirectMemoryStore>( memoryStore, offHeapStore,
                                                                           config.getCopyStrategy(),
                                                                           config.isCopyOnWrite(),
                                                                           config.isCopyOnRead() )
            {

                @Override
                public Object getMBean()
                {
                    return this.authority.getMBean();
                }

            };
        }
        return store;
    }

    /**
     * Creates a persitent-to-disk store for the given cache, using the given
     * disk path. Heap and disk usage are not tracked by the returned store.
     *
     * @param cache         cache that fronts this store
     * @param diskStorePath disk path to store data in
     * @return a fully initialized store
     */
    public static Store create( Ehcache cache, String diskStorePath )
    {
        return create( cache, diskStorePath, new UnboundedPool(), new UnboundedPool() );
    }

    private static MemoryStore createMemoryStore( Ehcache cache, Pool<PoolableStore> onHeapPool )
    {
        return MemoryStore.create( cache, onHeapPool );
    }

    private static DirectMemoryStore createOffHeapStore( Ehcache cache, boolean lowestTier )
    {
        Pool<PoolableStore> offHeapPool = null;
        if ( cache.getCacheConfiguration().getMaxBytesLocalOffHeap() == 0L )
        {
            offHeapPool = getOffHeapPool( cache.getCacheManager() );
        }
        return new DirectMemoryStore( cache, offHeapPool );
    }

    private static Pool<PoolableStore> getOffHeapPool( CacheManager manager )
    {
        Pool<PoolableStore> pool;
        synchronized ( OFFHEAP_POOLS )
        {
            pool = OFFHEAP_POOLS.get( manager );
            if ( pool == null )
            {
                pool = new UnboundedPool();
                OFFHEAP_POOLS.put( manager, pool );
            }
        }
        return pool;
    }

}
