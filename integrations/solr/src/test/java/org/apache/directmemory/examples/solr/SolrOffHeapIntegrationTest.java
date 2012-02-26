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

import static org.junit.Assert.assertTrue;

import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.util.TestHarness;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 */
public class SolrOffHeapIntegrationTest
{

    private static SolrOffHeapCache<Object, Object> solrOffHeapCache;

    private static TestHarness h;

    @BeforeClass
    public static void setUp()
    {
        String data = "target/data/expand";
        String config = SolrOffHeapCache.class.getResource( "/solr/config/solrconfig.xml" ).getFile();
        String schema = SolrOffHeapCache.class.getResource( "/solr/config/schema.xml" ).getFile();
        h = new TestHarness( data, config, schema );
        solrOffHeapCache = new SolrOffHeapCache<Object, Object>();
    }

    @AfterClass
    public static void tearDown()
    {
        solrOffHeapCache.getCacheService().clear();
    }

    @Test
    @Ignore // FIXME need TomNaso help - now I see why he needed a static CacheService reference
    public void testSimpleQuery()
        throws Exception
    {

        // add a doc to Solr
        h.validateAddDoc( "id", "1", "text", "something is happening here" );

        // make the query
        LocalSolrQueryRequest request = h.getRequestFactory( "standard", 0, 10 ).makeRequest( "q", "something" );
        String response = h.query( "standard", request );
        assertTrue( response != null );
        assertTrue( !response.contains( "error" ) );

        // check the cache has been hit
        assertTrue( solrOffHeapCache.getCacheService().entries() > 0 );


    }
}
