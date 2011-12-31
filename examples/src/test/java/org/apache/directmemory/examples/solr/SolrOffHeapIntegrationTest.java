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
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.util.TestHarness;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 */
public class SolrOffHeapIntegrationTest
{
    private TestHarness h;

    @Before
    public void setUp()
    {
        String data = "target/data/expand";
        String config = this.getClass().getResource( "/solr/config/solrconfig.xml" ).getFile();
        String schema = this.getClass().getResource( "/solr/config/schema.xml" ).getFile();
        this.h = new TestHarness( data, config, schema );
    }

    @Test
    public void testSimpleQuery()
    {
        try
        {
            // add a doc to Solr
            h.validateAddDoc( "id", "1", "text", "something is happening here" );

            // make the query
            LocalSolrQueryRequest request = h.getRequestFactory( "standard", 0, 100 ).makeRequest( "q", "something" );
            String response = h.query( "standard", request );
            assertTrue( response != null );
            assertTrue( !response.contains( "error" ) );

            // check the cache has been hit
            assertTrue( Cache.entries() > 0 );

        }
        catch ( Throwable e )
        {
            e.printStackTrace();
            fail( e.getLocalizedMessage() );
        }
    }
}
