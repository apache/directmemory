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

import org.apache.commons.lang.StringUtils;
import org.apache.directmemory.cache.CacheService;
import org.apache.directmemory.cache.CacheServiceImpl;
import org.apache.directmemory.memory.Pointer;
import org.apache.directmemory.server.commons.DirectMemoryCacheException;
import org.apache.directmemory.server.commons.DirectMemoryCacheRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * TODO add some listener plugin mechanism to store figures/statistics on cache access
 *
 * @author Olivier Lamy
 */
public class CacheServlet
    extends HttpServlet
{

    private Logger log = LoggerFactory.getLogger( getClass() );

    public static final String JAVA_SERIALIZED_OBJECT_CONTENT_TYPE_HEADER = "application/x-java-serialized-object";

    private CacheService cacheService = new CacheServiceImpl();

    private Map<String, CacheContentTypeHandler> contentTypeHandlers;


    @Override
    public void init( ServletConfig config )
        throws ServletException
    {
        super.init( config );
        // TODO some configuration for cacheService.init( .... ); different from sysproperties
        //int numberOfBuffers, int size, int initialCapacity, int concurrencyLevel
        int numberOfBuffers = Integer.getInteger( "directMemory.numberOfBuffers", 1000 );
        int size = Integer.getInteger( "directMemory.size", 10 );
        int initialCapacity =
            Integer.getInteger( "directMemory.initialCapacity", CacheService.DEFAULT_INITIAL_CAPACITY );
        int concurrencyLevel =
            Integer.getInteger( "directMemory.concurrencyLevel", CacheService.DEFAULT_CONCURRENCY_LEVEL );
        cacheService.init( numberOfBuffers, size, initialCapacity, concurrencyLevel );

        //

        contentTypeHandlers = new HashMap<String, CacheContentTypeHandler>( 1 );
        contentTypeHandlers.put( MediaType.APPLICATION_JSON, new JsonCacheContentTypeHandler() );
    }

    @Override
    public void destroy()
    {
        super.destroy();
    }

    @Override
    protected void doPost( HttpServletRequest req, HttpServletResponse resp )
        throws ServletException, IOException
    {
        this.doPut( req, resp );
    }

    @Override
    protected void doPut( HttpServletRequest req, HttpServletResponse resp )
        throws ServletException, IOException
    {
        //TODO check request content to send HttpServletResponse.SC_BAD_REQUEST
        // if missing parameter in json request

        String path = req.getPathInfo();
        String servletPath = req.getServletPath();
        String key = retrieveKeyFromPath( path );

        DirectMemoryCacheRequest cacheRequest = null;

        CacheContentTypeHandler contentTypeHandler = findPutCacheContentTypeHandler( req, resp );

        if ( contentTypeHandler == null )
        {
            resp.sendError( HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                            "Content-Type '" + req.getContentType() + "' not supported" );
            return;
        }
        try
        {
            cacheRequest = contentTypeHandler.handlePut( req, resp );
        }
        catch ( DirectMemoryCacheException e )
        {
            resp.sendError( HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getMessage() );
            return;
        }

        //if exists free first ?
        //if ( cacheService.retrieveByteArray( key ) == null )
        cacheService.putByteArray( key, cacheRequest.getCacheContent(), cacheRequest.getExpiresIn() );
    }

    protected CacheContentTypeHandler findPutCacheContentTypeHandler( HttpServletRequest req,
                                                                      HttpServletResponse response )
    {

        String contentType = req.getContentType();
        if ( StringUtils.startsWith( contentType, MediaType.APPLICATION_JSON ) )
        {
            // 	application/json
            return contentTypeHandlers.get( MediaType.APPLICATION_JSON );
        }
        return null;
    }

    @Override
    protected void doDelete( HttpServletRequest req, HttpServletResponse resp )
        throws ServletException, IOException
    {
        String path = req.getPathInfo();
        String servletPath = req.getServletPath();
        String key = retrieveKeyFromPath( path );

        // TODO if key == null -> BAD_REQUEST http response or SC_NO_CONTENT (olamy: I prefer SC_NO_CONTENT )
        Pointer pointer = cacheService.getPointer( key );
        if ( pointer == null )
        {
            resp.sendError( HttpServletResponse.SC_NO_CONTENT, "No content for key: " + key );
            return;
        }
        cacheService.free( pointer );
    }

    @Override
    protected void doGet( HttpServletRequest req, HttpServletResponse resp )
        throws ServletException, IOException
    {
        // url format = /cache/key so get the key from path
        String path = req.getPathInfo();
        String servletPath = req.getServletPath();
        String key = retrieveKeyFromPath( path );

        if ( StringUtils.isEmpty( key ) )
        {
            resp.sendError( HttpServletResponse.SC_BAD_REQUEST, "key missing in path" );
            return;
        }

        byte[] bytes = cacheService.retrieveByteArray( key );

        log.debug( "content size {} for key {}", ( bytes == null ? "null" : bytes.length ), key );

        if ( bytes == null || bytes.length == 0 )
        {
            resp.sendError( HttpServletResponse.SC_NO_CONTENT, "No content for key: " + key );
            return;
        }

        String acceptContentType = req.getHeader( "Accept" );

        if ( StringUtils.isEmpty( acceptContentType ) )
        {
            resp.sendError( HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                            "you must specify Accept with Content-Type you want in the response" );
            return;
        }

        CacheContentTypeHandler contentTypeHandler = findGetCacheContentTypeHandler( req, resp );

        if ( contentTypeHandler == null )
        {
            resp.sendError( HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                            "Content-Type: " + acceptContentType + " not supported" );
            return;
        }

        try
        {
            byte[] respBytes =
                contentTypeHandler.handleGet( new DirectMemoryCacheRequest().setKey( key ), bytes, resp );
            resp.getOutputStream().write( respBytes );
        }
        catch ( DirectMemoryCacheException e )
        {
            resp.sendError( HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getMessage() );
        }


    }

    protected CacheContentTypeHandler findGetCacheContentTypeHandler( HttpServletRequest req,
                                                                      HttpServletResponse response )
    {

        String acceptContentType = req.getHeader( "Accept" );
        if ( StringUtils.contains( acceptContentType, MediaType.APPLICATION_JSON ) )
        {
            // 	application/json
            return contentTypeHandlers.get( MediaType.APPLICATION_JSON );
        }
        return null;
    }

    /**
     * protected for unit test reason
     *
     * @param path
     * @return
     */

    protected String retrieveKeyFromPath( String path )
    {
        if ( StringUtils.endsWith( path, "/" ) )
        {
            return StringUtils.substringAfterLast( StringUtils.substringBeforeLast( path, "/" ), "/" );
        }
        return StringUtils.substringAfterLast( path, "/" );
    }
}
