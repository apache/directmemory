package org.apache.directmemory.server.commons;
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

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import static org.junit.Assert.*;

/**
 * @author Olivier Lamy
 */
public class DirectMemoryParserTest
{
    @Test
    public void parseRequest()
        throws Exception
    {
        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream( "cache-rq.json" );
        assertNotNull( is );
        DirectMemoryRequest dmRq = DirectMemoryParser.instance().buildRequest( is );
        assertNotNull( dmRq );

        assertEquals( "101", dmRq.getKey() );
        assertEquals( true, dmRq.isUpdate() );
        assertEquals( 123, dmRq.getExpiresIn() );
    }

    @Test
    public void parseResponse()
        throws Exception
    {
        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream( "cache-rs.json" );
        assertNotNull( is );
        DirectMemoryResponse dmRs = DirectMemoryParser.instance().buildResponse( is );
        assertNotNull( dmRs );

        assertEquals( "foo", dmRs.getKey() );
        assertEquals( true, dmRs.isFound() );
        assertEquals( false, dmRs.isUpdated() );
        assertEquals( "foo bar", new String( dmRs.getCacheContent() ) );
    }

    @Test
    public void parseEmptyRequest()
        throws Exception
    {
        InputStream is = new ByteArrayInputStream( new byte[0] );
        DirectMemoryRequest dmRq = DirectMemoryParser.instance().buildRequest( is );
        assertNotNull( dmRq );
        assertNull( dmRq.getCacheContent() );

    }
}
