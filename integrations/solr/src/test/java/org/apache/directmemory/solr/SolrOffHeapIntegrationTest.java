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
package org.apache.directmemory.solr;

import java.util.Map;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrInfoMBean;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 */
public class SolrOffHeapIntegrationTest
        extends SolrTestCaseJ4 {

    @BeforeClass
    public static void beforeClass() throws Exception {
        initCore("solrconfig.xml", "schema.xml", getFile("solr").getAbsolutePath());
    }

    @Test
    public void testSingleQuery()
        throws Exception
    {

        // add a doc to Solr
        assertU(adoc("id", "1", "text", "something is happening here"));
        assertU(commit());
        // make the query
        assertQ(req("text:something"), "//*[@numFound='1']");

        Map<String, SolrInfoMBean> infoRegistry = h.getCore().getInfoRegistry();

        // check the stats of the queryResultCache
        SolrInfoMBean solrInfoMBean = infoRegistry.get("queryResultCache");
        NamedList stats = solrInfoMBean.getStatistics();
        Long hits = (Long) stats.get("hits");
        assertEquals(Long.valueOf(0l), hits);
        Long lookups = (Long) stats.get("lookups");
        assertEquals(Long.valueOf(1l), lookups);
        Long inserts = (Long) stats.get("inserts");
        assertEquals(Long.valueOf(1l), inserts);

    }

}
