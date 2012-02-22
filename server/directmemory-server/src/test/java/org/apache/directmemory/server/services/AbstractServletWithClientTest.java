package org.apache.directmemory.server.services;
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

import org.apache.catalina.Context;
import org.apache.catalina.startup.Tomcat;
import org.apache.directmemory.serialization.SerializerFactory;
import org.apache.directmemory.server.client.DefaultDirectMemoryServerClient;
import org.apache.directmemory.server.client.DirectMemoryHttpClient;
import org.apache.directmemory.server.client.DirectMemoryServerClient;
import org.apache.directmemory.server.client.DirectMemoryServerClientConfiguration;
import org.apache.directmemory.server.client.HttpClientDirectMemoryHttpClient;
import org.apache.directmemory.server.commons.DirectMemoryCacheRequest;
import org.apache.directmemory.server.commons.DirectMemoryCacheResponse;
import org.apache.directmemory.server.commons.ExchangeType;
import org.apache.directmemory.test.Wine;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

/**
 * @author Olivier Lamy
 */
public abstract class AbstractServletWithClientTest
{
    Logger log = LoggerFactory.getLogger( getClass() );

    Tomcat tomcat;

    private int port;

    DirectMemoryServerClient client;

    protected abstract ExchangeType getExchangeType();

    @Before
    public void initialize()
        throws Exception
    {
        tomcat = new Tomcat();
        tomcat.setBaseDir( System.getProperty( "java.io.tmpdir" ) );
        tomcat.setPort( 0 );

        Context context = tomcat.addContext( "/direct-memory", System.getProperty( "java.io.tmpdir" ) );

        tomcat.addServlet( context, CacheServlet.class.getName(), new CacheServlet() );
        context.addServletMapping( "/CacheServlet/*", CacheServlet.class.getName() );

        tomcat.start();

        port = tomcat.getConnector().getLocalPort();

        log.info( "Tomcat started on port:" + port );

        // START SNIPPET: client-configuration
        DirectMemoryServerClientConfiguration configuration =
            new DirectMemoryServerClientConfiguration()
                .setHost( "localhost" )
                .setPort( port )
                .setHttpPath( "/direct-memory/CacheServlet" )
                .setSerializer( SerializerFactory.createNewSerializer() )
                .setExchangeType( getExchangeType() );

        DirectMemoryHttpClient httpClient = HttpClientDirectMemoryHttpClient.instance( configuration );
        configuration.setDirectMemoryHttpClient( httpClient );

        client = DefaultDirectMemoryServerClient.instance( configuration );
        // END SNIPPET: client-configuration
    }

    public void shutdown()
        throws Exception
    {
        tomcat.stop();
    }

    @Test
    public void putAndGet()
        throws Exception
    {
        // START SNIPPET: client-put

        Wine bordeaux = new Wine( "Bordeaux", "very great wine" );
        client.put( new DirectMemoryCacheRequest<Wine>( "bordeaux", bordeaux ) );

        // END SNIPPET: client-put

        // START SNIPPET: client-get
        DirectMemoryCacheRequest rq = new DirectMemoryCacheRequest( "bordeaux", Wine.class );

        DirectMemoryCacheResponse<Wine> response = client.retrieve( rq );

        assertTrue( response.isFound() );
        Wine wine = response.getResponse();
        // END SNIPPET: client-get
        assertEquals( "Bordeaux", wine.getName() );
        assertEquals( "very great wine", wine.getDescription() );
    }

    @Test
    public void getNotFound()
        throws Exception
    {

        DirectMemoryCacheResponse<Wine> response =
            client.retrieve( new DirectMemoryCacheRequest( "Italian wine better than French", Wine.class ) );

        // due to the key used the server should response BAD Request but it says not found
        assertFalse( response.isFound() );
        assertNull( response.getCacheContent() );
    }

    @Test
    public void putAndGetAndDelete()
        throws Exception
    {
        Wine bordeaux = new Wine( "Bordeaux", "very great wine" );

        client.put( new DirectMemoryCacheRequest<Wine>( "bordeaux", bordeaux ) );

        DirectMemoryCacheResponse<Wine> response =
            client.retrieve( new DirectMemoryCacheRequest( "bordeaux", Wine.class ) );

        assertTrue( response.isFound() );
        Wine wine = response.getResponse();
        assertEquals( "Bordeaux", wine.getName() );
        assertEquals( "very great wine", wine.getDescription() );

        // START SNIPPET: client-delete

        DirectMemoryCacheResponse deleteResponse = client.delete( new DirectMemoryCacheRequest<Wine>( "bordeaux" ) );
        assertTrue( deleteResponse.isDeleted() );

        // END SNIPPET: client-delete

        response = client.retrieve( new DirectMemoryCacheRequest( "bordeaux", Wine.class ) );

        assertFalse( response.isFound() );
        wine = response.getResponse();
        assertNull( wine );
    }

    @Test
    public void deleteNotFound()
        throws Exception
    {
        DirectMemoryCacheResponse deleteResponse = client.delete( new DirectMemoryCacheRequest<Wine>( "fofoofofof" ) );
        assertFalse( deleteResponse.isDeleted() );
    }

    @Test
    public void putSmallExpiresAndGetNotFound()
        throws Exception
    {

        DirectMemoryCacheResponse deleteResponse = client.delete( new DirectMemoryCacheRequest<Wine>( "bordeaux" ) );
        Wine bordeaux = new Wine( "Bordeaux", "very great wine" );
        client.put( new DirectMemoryCacheRequest<Wine>( "bordeaux", bordeaux ).setExpiresIn( 1000 ) );

        DirectMemoryCacheRequest rq = new DirectMemoryCacheRequest( "bordeaux", Wine.class );

        DirectMemoryCacheResponse<Wine> response = client.retrieve( rq );

        assertTrue( response.isFound() );
        Wine wine = response.getResponse();

        assertEquals( "Bordeaux", wine.getName() );
        assertEquals( "very great wine", wine.getDescription() );

        Thread.sleep( 10001 );

        rq = new DirectMemoryCacheRequest( "bordeaux", Wine.class );

        response = client.retrieve( rq );

        assertFalse( response.isFound() );

        assertNull( response.getResponse() );
    }
}
