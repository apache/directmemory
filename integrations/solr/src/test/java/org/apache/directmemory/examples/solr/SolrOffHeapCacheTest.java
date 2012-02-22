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

import org.apache.directmemory.serialization.StandardSerializer;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.search.QueryResultKey;
import org.apache.solr.search.function.DocValues;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Testcase for {@link SolrOffHeapCache}
 */
public class SolrOffHeapCacheTest
{

    private Logger log = LoggerFactory.getLogger( getClass() );

    private SolrOffHeapCache solrOffHeapCache;

    @Before
    public void setUp()
    {
        solrOffHeapCache = new SolrOffHeapCache();
        Map<String, String> args = new HashMap<String, String>();
        args.put( "size", "10000" );
        args.put( "initialSize", "1000" );
        args.put( "serializerClassName", StandardSerializer.class.getName() );
        try
        {
            solrOffHeapCache.init( args, null, null );
        }
        catch ( NoClassDefFoundError e )
        {
            log.error( e.getMessage(), e );
            throw e;
        }
    }

    @After
    public void tearDown()
    {
        solrOffHeapCache.clear();
        solrOffHeapCache.close();
    }

    @Test
    public void testStatisticsWhenCacheNotUsedYet()
        throws Exception
    {

        NamedList stats = solrOffHeapCache.getStatistics();
        assertNotNull( stats );
        assertEquals( 0l, stats.get( "lookups" ) );
        assertEquals( 0l, stats.get( "evictions" ) );
        assertEquals( 0l, stats.get( "hits" ) );
        assertEquals( 0l, stats.get( "inserts" ) );
    }

    @Test
    public void testPut()
        throws Exception
    {

        QueryResultKey queryResultKey =
            new QueryResultKey( new MatchAllDocsQuery(), new ArrayList<Query>(), new Sort(), 1 );
        DocValues docValues = new DocValues()
        {
            @Override
            public String toString( int doc )
            {
                return doc + "asd";
            }
        };

        solrOffHeapCache.put( queryResultKey, docValues );

    }
}
