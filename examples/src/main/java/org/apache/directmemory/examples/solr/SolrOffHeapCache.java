/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.directmemory.examples.solr;

import org.apache.directmemory.cache.Cache;
import org.apache.directmemory.measures.Ram;
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
public class SolrOffHeapCache<K, V> implements SolrCache<K, V> {

  private static class CumulativeStats {
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

  @Override
  public Object init(Map args, Object persistence, CacheRegenerator regenerator) {
    Object buffers = args.get("buffers");
    String size = String.valueOf(args.get("size"));
    Cache.init(buffers != null ? Integer.valueOf(String.valueOf(buffers)) : 1, Ram.Mb(Double.valueOf(size) / 512));

    state = State.CREATED;
    this.regenerator = regenerator;
    name = (String) args.get("name");
    String str = size;
    final int limit = str == null ? 1024 : Integer.parseInt(str);
    str = (String) args.get("initialSize");
    final int initialSize = Math.min(str == null ? 1024 : Integer.parseInt(str), limit);
    str = (String) args.get("autowarmCount");
    autowarmCount = str == null ? 0 : Integer.parseInt(str);

    description = "Solr OffHeap Cache(maxSize=" + limit + ", initialSize=" + initialSize;
    if (autowarmCount > 0) {
      description += ", autowarmCount=" + autowarmCount
              + ", regenerator=" + regenerator;
    }
    description += ')';

    if (persistence == null) {
      // must be the first time a cache of this type is being created
      persistence = new CumulativeStats();
    }

    stats = (CumulativeStats) persistence;

    return persistence;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public int size() {
    return Long.valueOf(Cache.entries()).intValue();
  }

  @Override
  public V put(K key, V value) {
    return (V) Cache.put(String.valueOf(key), value);
  }

  @Override
  public V get(K key) {
    return (V) Cache.retrieve(String.valueOf(key));
  }

  @Override
  public void clear() {
    synchronized (this) {
      Cache.clear();
    }
  }

  @Override
  public void setState(State state) {
    this.state = state;
  }

  @Override
  public State getState() {
    return state;
  }

  @Override
  public void warm(SolrIndexSearcher searcher, SolrCache<K, V> old) throws IOException {
    // it looks like there is no point in warming an off heap item
  }

  @Override
  public void close() {
  }

  @Override
  public String getName() {
    return SolrOffHeapCache.class.getName();
  }

  @Override
  public String getVersion() {
    return SolrCore.version;
  }

  public String getDescription() {
    return description;
  }

  public Category getCategory() {
    return Category.CACHE;
  }

  @Override
  public String getSourceId() {
    return null;
  }

  @Override
  public String getSource() {
    return null;
  }

  @Override
  public URL[] getDocs() {
    return new URL[0];
  }

  public NamedList getStatistics() {
    NamedList lst = new SimpleOrderedMap();
    synchronized (this) {
      lst.add("lookups", lookups);
      lst.add("hits", hits);
      lst.add("hitratio", calcHitRatio(lookups, hits));
      lst.add("inserts", inserts);
      lst.add("evictions", evictions);
      lst.add("size", Cache.entries());
    }

    lst.add("warmupTime", warmupTime);

    long clookups = stats.lookups.get();
    long chits = stats.hits.get();
    lst.add("cumulative_lookups", clookups);
    lst.add("cumulative_hits", chits);
    lst.add("cumulative_hitratio", calcHitRatio(clookups, chits));
    lst.add("cumulative_inserts", stats.inserts.get());
    lst.add("cumulative_evictions", stats.evictions.get());

    return lst;
  }

  private Object calcHitRatio(long clookups, long chits) {
    int ones = (int) (hits * 100 / lookups);
    int tenths = (int) (hits * 1000 / lookups) - ones * 10;
    return Integer.toString(ones) + '.' + tenths;
  }

  @Override
  public String toString() {
    return name + getStatistics().toString();
  }
}
