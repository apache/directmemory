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

import org.apache.directmemory.serialization.Serializer;
import org.apache.directmemory.serialization.SerializerFactory;
import org.apache.directmemory.server.commons.DirectMemoryCacheParser;
import org.apache.directmemory.server.commons.DirectMemoryCacheRequest;
import org.apache.directmemory.server.commons.DirectMemoryCacheResponse;
import org.apache.directmemory.server.commons.DirectMemoryCacheWriter;
import org.apache.directmemory.test.Wine;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.mock.web.MockServletConfig;
import org.springframework.mock.web.MockServletContext;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;
import java.io.ByteArrayInputStream;

import static org.junit.Assert.assertEquals;

/**
 * @author Olivier Lamy
 */
public class CacheServletTest
{
    private Logger log = LoggerFactory.getLogger( getClass() );

    CacheServlet cacheServlet = new CacheServlet();

    DirectMemoryCacheWriter writer = DirectMemoryCacheWriter.instance();

    DirectMemoryCacheParser parser = DirectMemoryCacheParser.instance();

    @Before
    public void init()
        throws Exception
    {

        MockServletContext mockServletContext = new MockServletContext();
        mockServletContext.setContextPath( "direct-memory" );

        MockServletConfig mockServletConfig = new MockServletConfig( mockServletContext );

        cacheServlet.init( mockServletConfig );
    }

    @Test
    public void badRequest()
        throws Exception
    {

        MockHttpServletRequest request = new MockHttpServletRequest();
        request.addHeader( "Accept", MediaType.APPLICATION_JSON );

        MockHttpServletResponse response = new MockHttpServletResponse();

        cacheServlet.doGet( request, response );

        assertEquals( HttpServletResponse.SC_BAD_REQUEST, response.getStatus() );


    }

    @Test
    public void keyNotFound()
        throws Exception
    {

        MockHttpServletRequest request = new MockHttpServletRequest();
        request.addHeader( "Accept", MediaType.APPLICATION_JSON );

        request.setServletPath( "cache" );

        request.setPathInfo( "/foo" );

        MockHttpServletResponse response = new MockHttpServletResponse();

        cacheServlet.doGet( request, response );

        assertEquals( HttpServletResponse.SC_NO_CONTENT, response.getStatus() );

    }

    @Test
    public void storeObject()
        throws Exception
    {

        Serializer serializer = SerializerFactory.createNewSerializer();

        Wine bordeaux = new Wine( "Bordeaux", "very great wine" );

        DirectMemoryCacheRequest directMemoryCacheRequest =
            new DirectMemoryCacheRequest().setKey( "bordeaux" ).setCacheContent( serializer.serialize( bordeaux ) );

        String rq = writer.generateJsonRequest( directMemoryCacheRequest );

        MockHttpServletRequest putRequest = new MockHttpServletRequest();

        putRequest.setContentType( MediaType.APPLICATION_JSON );

        putRequest.setServletPath( "cache" );

        putRequest.setPathInfo( "/bordeaux" );

        putRequest.setContent( rq.getBytes() );

        MockHttpServletResponse putResponse = new MockHttpServletResponse();

        cacheServlet.doPut( putRequest, putResponse );

        assertEquals( HttpServletResponse.SC_OK, putResponse.getStatus() );

        // now retrieve the content

        MockHttpServletRequest getRequest = new MockHttpServletRequest();

        getRequest.addHeader( "Accept", MediaType.APPLICATION_JSON );

        getRequest.setPathInfo( "/bordeaux" );

        MockHttpServletResponse getResponse = new MockHttpServletResponse();

        cacheServlet.doGet( getRequest, getResponse );

        assertEquals( HttpServletResponse.SC_OK, getResponse.getStatus() );

        assertEquals( MediaType.APPLICATION_JSON, getResponse.getContentType() );

        DirectMemoryCacheResponse response =
            parser.buildResponse( new ByteArrayInputStream( getResponse.getContentAsByteArray() ) );

        Wine wineFromCache = serializer.deserialize( response.getCacheContent(), Wine.class );

        assertEquals( bordeaux.getName(), wineFromCache.getName() );
        assertEquals( bordeaux.getDescription(), wineFromCache.getDescription() );

    }

    @Test
    public void storeExpiredObject()
        throws Exception
    {

        Serializer serializer = SerializerFactory.createNewSerializer();

        Wine bordeaux = new Wine( "Bordeaux", "very great wine" );

        DirectMemoryCacheRequest directMemoryCacheRequest =
            new DirectMemoryCacheRequest().setKey( "bordeaux" ).setCacheContent(
                serializer.serialize( bordeaux ) ).setExpiresIn( 3 );

        String rq = writer.generateJsonRequest( directMemoryCacheRequest );

        MockHttpServletRequest putRequest = new MockHttpServletRequest();

        putRequest.setContentType( MediaType.APPLICATION_JSON );

        putRequest.setServletPath( "cache" );

        putRequest.setPathInfo( "/bordeaux" );

        putRequest.setContent( rq.getBytes() );

        MockHttpServletResponse putResponse = new MockHttpServletResponse();

        cacheServlet.doPut( putRequest, putResponse );

        assertEquals( HttpServletResponse.SC_OK, putResponse.getStatus() );

        Thread.sleep( 10 );

        // now retrieve the content

        MockHttpServletRequest getRequest = new MockHttpServletRequest();

        getRequest.addHeader( "Accept", MediaType.APPLICATION_JSON );

        getRequest.setPathInfo( "/bordeaux" );

        MockHttpServletResponse getResponse = new MockHttpServletResponse();

        cacheServlet.doGet( getRequest, getResponse );

        assertEquals( HttpServletResponse.SC_NO_CONTENT, getResponse.getStatus() );


    }


    @Test
    public void storeObjectThenRemove()
        throws Exception
    {

        Serializer serializer = SerializerFactory.createNewSerializer();

        Wine bordeaux = new Wine( "Bordeaux", "very great wine" );

        DirectMemoryCacheRequest directMemoryCacheRequest =
            new DirectMemoryCacheRequest().setKey( "bordeaux" ).setCacheContent( serializer.serialize( bordeaux ) );

        String rq = writer.generateJsonRequest( directMemoryCacheRequest );

        MockHttpServletRequest putRequest = new MockHttpServletRequest();

        putRequest.setContentType( MediaType.APPLICATION_JSON );

        putRequest.setServletPath( "cache" );

        putRequest.setPathInfo( "/bordeaux" );

        putRequest.setContent( rq.getBytes() );

        MockHttpServletResponse putResponse = new MockHttpServletResponse();

        cacheServlet.doPut( putRequest, putResponse );

        assertEquals( HttpServletResponse.SC_OK, putResponse.getStatus() );

        // now retrieve the content

        MockHttpServletRequest getRequest = new MockHttpServletRequest();

        getRequest.addHeader( "Accept", MediaType.APPLICATION_JSON );

        getRequest.setPathInfo( "/bordeaux" );

        MockHttpServletResponse getResponse = new MockHttpServletResponse();

        cacheServlet.doGet( getRequest, getResponse );

        assertEquals( HttpServletResponse.SC_OK, getResponse.getStatus() );

        assertEquals( MediaType.APPLICATION_JSON, getResponse.getContentType() );

        DirectMemoryCacheResponse response =
            parser.buildResponse( new ByteArrayInputStream( getResponse.getContentAsByteArray() ) );

        Wine wineFromCache = serializer.deserialize( response.getCacheContent(), Wine.class );

        assertEquals( bordeaux.getName(), wineFromCache.getName() );
        assertEquals( bordeaux.getDescription(), wineFromCache.getDescription() );

        // now delete the content

        MockHttpServletRequest deleteRq = new MockHttpServletRequest();

        deleteRq.setPathInfo( "/bordeaux" );

        MockHttpServletResponse deleteResponse = new MockHttpServletResponse();

        cacheServlet.doDelete( deleteRq, deleteResponse );

        assertEquals( HttpServletResponse.SC_OK, deleteResponse.getStatus() );

        // now try again a read MUST be not content

        getRequest = new MockHttpServletRequest();

        getRequest.addHeader( "Accept", MediaType.APPLICATION_JSON );

        getRequest.setPathInfo( "/bordeaux" );

        getResponse = new MockHttpServletResponse();

        cacheServlet.doGet( getRequest, getResponse );

        assertEquals( HttpServletResponse.SC_NO_CONTENT, getResponse.getStatus() );

    }

}
