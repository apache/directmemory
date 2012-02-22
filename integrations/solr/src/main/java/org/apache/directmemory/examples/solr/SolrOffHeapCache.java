package org.apache.directmemory.examples.solr;

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

import org.apache.directmemory.cache.CacheService;
import org.apache.directmemory.cache.CacheServiceImpl;
import org.apache.directmemory.measures.Monitor;
import org.apache.directmemory.measures.Ram;
import org.apache.directmemory.serialization.Serializer;
import org.apache.directmemory.serialization.SerializerFactory;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.SolrCore;
import org.apache.solr.search.CacheRegenerator;
import org.apache.solr.search.SolrCache;
import org.apache.solr.search.SolrIndexSearcher;

import java.io.IOException;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A {@link SolrCache} based on Apache DirectMemory
 */
public class SolrOffHeapCache<K, V>
    implements SolrCache<K, V>
{

    private static class CumulativeStats
    {
        AtomicLong lookups = new AtomicLong();

        AtomicLong hits = new AtomicLong();

        AtomicLong inserts = new AtomicLong();

        AtomicLong evictions = new AtomicLong();
    }

    private CumulativeStats stats;

    private long lookups;

    private long hits;

    private long inserts;

    private long evictions;

    private long warmupTime = 0;

    private String name;

    private int autowarmCount;

    private State state;

    private CacheRegenerator regenerator;

    private String description = "DM Cache";

    private static CacheService cacheService = new CacheServiceImpl();

    public static CacheService getCacheService()
    {
        return cacheService;
    }

    @Override
    public Object init( Map args, Object persistence, CacheRegenerator regenerator )
    {
        Object buffers = args.get( "buffers" );
        String sizeStr = String.valueOf( args.get( "size" ) );
        Integer capacity = Integer.parseInt( String.valueOf( args.get( "initialSize" ) ) );

        int numberOfBuffers = buffers != null ? Integer.valueOf( String.valueOf( buffers ) ) : 1,
            size = Ram.Mb( Double.valueOf( sizeStr ) / 512 ),
            initialCapacity = Ram.Mb( Double.valueOf( capacity ) / 512 ),
            concurrencyLevel = CacheService.DEFAULT_CONCURRENCY_LEVEL;

        cacheService = new CacheServiceImpl();
        //Cache.init( numberOfBuffers, size, initialCapacity, concurrencyLevel );
        cacheService.init( numberOfBuffers, size, initialCapacity, concurrencyLevel );

        String serializerClassName = (String) args.get( "serializerClassName" );
        if ( serializerClassName != null )
        {
            Serializer serializer = SerializerFactory.createNewSerializer( serializerClassName );
            if ( serializer == null )
            {
                serializer = SerializerFactory.createNewSerializer();
            }
            cacheService.setSerializer( serializer );
        }

        state = State.CREATED;
        this.regenerator = regenerator;
        name = (String) args.get( "name" );
        final int limit = sizeStr == null ? 1024 : Integer.parseInt( sizeStr );
        String str = (String) args.get( "initialSize" );
        final int initialSize = Math.min( str == null ? 1024 : Integer.parseInt( str ), limit );
        str = (String) args.get( "autowarmCount" );
        autowarmCount = str == null ? 0 : Integer.parseInt( str );

        description = "Solr OffHeap Cache(maxSize=" + limit + ", initialSize=" + initialSize;
        if ( autowarmCount > 0 )
        {
            description += ", autowarmCount=" + autowarmCount + ", regenerator=" + regenerator;
        }
        description += ')';

        if ( persistence == null )
        {
            // must be the first time a cache of this type is being created
            persistence = new CumulativeStats();
        }

        stats = (CumulativeStats) persistence;

        return persistence;
    }

    @Override
    public String name()
    {
        return name;
    }

    @Override
    public int size()
    {
        return Long.valueOf( cacheService.entries() ).intValue();
    }

    @Override
    public V put( K key, V value )
    {
        synchronized ( this )
        {
            if ( state == State.LIVE )
            {
                stats.inserts.incrementAndGet();
            }

            inserts++;
            return (V) cacheService.put( String.valueOf( key ), value );
        }
    }

    @Override
    public V get( K key )
    {
        synchronized ( this )
        {
            V val = (V) cacheService.retrieve( String.valueOf( key ) );
            if ( state == State.LIVE )
            {
                // only increment lookups and hits if we are live.
                lookups++;
                stats.lookups.incrementAndGet();
                if ( val != null )
                {
                    hits++;
                    stats.hits.incrementAndGet();
                }
            }
            return val;
        }
    }

    @Override
    public void clear()
    {
        synchronized ( this )
        {
            cacheService.clear();
        }
    }

    @Override
    public void setState( State state )
    {
        this.state = state;
    }

    @Override
    public State getState()
    {
        return state;
    }

    @Override
    public void warm( SolrIndexSearcher searcher, SolrCache<K, V> old )
        throws IOException
    {
        // it looks like there is no point in warming an off heap item
    }

    @Override
    public void close()
    {
        cacheService.dump();
        Monitor.dump();
    }

    @Override
    public String getName()
    {
        return SolrOffHeapCache.class.getName();
    }

    @Override
    public String getVersion()
    {
        return SolrCore.version;
    }

    public String getDescription()
    {
        return description;
    }

    public Category getCategory()
    {
        return Category.CACHE;
    }

    @Override
    public String getSourceId()
    {
        return null;
    }

    @Override
    public String getSource()
    {
        return null;
    }

    @Override
    public URL[] getDocs()
    {
        return new URL[0];
    }

    public NamedList getStatistics()
    {
        NamedList lst = new SimpleOrderedMap();
        synchronized ( this )
        {
            lst.add( "lookups", lookups );
            lst.add( "hits", hits );
            lst.add( "hitratio", calcHitRatio( lookups, hits ) );
            lst.add( "inserts", inserts );
            lst.add( "evictions", evictions );
            lst.add( "size", cacheService.entries() );
        }

        lst.add( "warmupTime", warmupTime );

        long clookups = stats.lookups.get();
        long chits = stats.hits.get();
        lst.add( "cumulative_lookups", clookups );
        lst.add( "cumulative_hits", chits );
        lst.add( "cumulative_hitratio", calcHitRatio( clookups, chits ) );
        lst.add( "cumulative_inserts", stats.inserts.get() );
        lst.add( "cumulative_evictions", stats.evictions.get() );

        return lst;
    }

    private Object calcHitRatio( long clookups, long chits )
    {
        if ( lookups == 0 )
        {
            return "0.00";
        }
        if ( lookups == hits )
        {
            return "1.00";
        }
        int hundredths = (int) ( hits * 100 / lookups );   // rounded down
        if ( hundredths < 10 )
        {
            return "0.0" + hundredths;
        }
        return "0." + hundredths;
    }

    @Override
    public String toString()
    {
        return name + getStatistics().toString();
    }
}
