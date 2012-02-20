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
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

/**
 * @author Olivier Lamy
 */
public class ServletWithClientTest
{
    Logger log = LoggerFactory.getLogger( getClass() );

    Tomcat tomcat;

    private int port;

    DirectMemoryServerClient client;


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

        DirectMemoryServerClientConfiguration configuration =
            new DirectMemoryServerClientConfiguration().setHost( "localhost" ).setPort( port ).setHttpPath(
                "/direct-memory/CacheServlet" );
        DirectMemoryHttpClient httpClient = HttpClientDirectMemoryHttpClient.instance( configuration );
        configuration.setDirectMemoryHttpClient( httpClient );

        client = DefaultDirectMemoryServerClient.instance( configuration );

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
        Wine bordeaux = new Wine( "Bordeaux", "very great wine" );

        client.put( new DirectMemoryCacheRequest<Wine>().setObject( bordeaux ).setKey( "bordeaux" ).setSerializer(
            SerializerFactory.createNewSerializer() ).setExchangeType( ExchangeType.JSON ) );

        DirectMemoryCacheResponse<Wine> response = client.retrieve(
            new DirectMemoryCacheRequest().setKey( "bordeaux" ).setSerializer(
                SerializerFactory.createNewSerializer() ).setExchangeType( ExchangeType.JSON ).setObjectClass(
                Wine.class ) );

        assertTrue( response.isFound() );
        Wine wine = response.getResponse();
        assertEquals( "Bordeaux", wine.getName() );
        assertEquals( "very great wine", wine.getDescription() );
    }

    @Test
    public void getNotFound()
        throws Exception
    {

        DirectMemoryCacheResponse<Wine> response = client.retrieve(
            new DirectMemoryCacheRequest().setKey( "Italian wine better than French" ).setSerializer(
                SerializerFactory.createNewSerializer() ).setExchangeType( ExchangeType.JSON ).setObjectClass(
                Wine.class ) );

        // due to the key used the server should response BAD Request but it says not found
        assertFalse( response.isFound() );
        assertNull( response.getCacheContent() );
    }

    @Test
    public void putAndGetAndDelete()
        throws Exception
    {
        Wine bordeaux = new Wine( "Bordeaux", "very great wine" );

        client.put( new DirectMemoryCacheRequest<Wine>().setObject( bordeaux ).setKey( "bordeaux" ).setSerializer(
            SerializerFactory.createNewSerializer() ).setExchangeType( ExchangeType.JSON ) );

        DirectMemoryCacheResponse<Wine> response = client.retrieve(
            new DirectMemoryCacheRequest().setKey( "bordeaux" ).setSerializer(
                SerializerFactory.createNewSerializer() ).setExchangeType( ExchangeType.JSON ).setObjectClass(
                Wine.class ) );

        assertTrue( response.isFound() );
        Wine wine = response.getResponse();
        assertEquals( "Bordeaux", wine.getName() );
        assertEquals( "very great wine", wine.getDescription() );

        DirectMemoryCacheResponse deleteResponse =
            client.delete( new DirectMemoryCacheRequest<Wine>().setKey( "bordeaux" ) );
        assertTrue( deleteResponse.isDeleted() );

        response = client.retrieve( new DirectMemoryCacheRequest().setKey( "bordeaux" ).setSerializer(
            SerializerFactory.createNewSerializer() ).setExchangeType( ExchangeType.JSON ).setObjectClass(
            Wine.class ) );

        assertFalse( response.isFound() );
        wine = response.getResponse();
        assertNull( wine );
    }

    @Test
    public void deleteNotFound()
        throws Exception
    {
        DirectMemoryCacheResponse deleteResponse =
            client.delete( new DirectMemoryCacheRequest<Wine>().setKey( "fofoofofof" ) );
        assertFalse( deleteResponse.isDeleted() );
    }
}
