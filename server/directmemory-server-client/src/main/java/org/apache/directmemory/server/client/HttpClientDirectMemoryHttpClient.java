package org.apache.directmemory.server.client;
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

import org.apache.directmemory.server.commons.DirectMemoryException;
import org.apache.directmemory.server.commons.DirectMemoryHttpConstants;
import org.apache.directmemory.server.commons.DirectMemoryRequest;
import org.apache.directmemory.server.commons.DirectMemoryResponse;
import org.apache.directmemory.server.commons.ExchangeType;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * @author Olivier Lamy
 */
public class HttpClientDirectMemoryHttpClient
    extends AbstractDirectMemoryHttpClient
    implements DirectMemoryHttpClient
{
    private Logger log = LoggerFactory.getLogger( getClass() );

    public static HttpClientDirectMemoryHttpClient instance( DirectMemoryClientConfiguration configuration )
    {
        return new HttpClientDirectMemoryHttpClient( configuration );
    }

    private HttpClient httpClient;


    public HttpClientDirectMemoryHttpClient( DirectMemoryClientConfiguration configuration )
    {
        super( configuration );
    }

    @Override
    public void configure( DirectMemoryClientConfiguration configuration )
        throws DirectMemoryException
    {
        this.configuration = configuration;
        ThreadSafeClientConnManager threadSafeClientConnManager = new ThreadSafeClientConnManager();
        threadSafeClientConnManager.setDefaultMaxPerRoute( configuration.getMaxConcurentConnection() );
        this.httpClient = new DefaultHttpClient( threadSafeClientConnManager );
    }

    @Override
    public DirectMemoryResponse put( DirectMemoryRequest request )
        throws DirectMemoryException
    {
        String uri = buildRequestWithKey( request );
        log.debug( "put request to: {}", uri );

        HttpPut httpPut = new HttpPut( uri );
        httpPut.addHeader( "Content-Type", getRequestContentType( request ) );

        if ( request.getExpiresIn() > 0 )
        {
            httpPut.addHeader( DirectMemoryHttpConstants.EXPIRES_IN_HTTP_HEADER,
                               Integer.toString( request.getExpiresIn() ) );
        }

        if ( request.getExchangeType() == ExchangeType.TEXT_PLAIN )
        {
            httpPut.addHeader( DirectMemoryHttpConstants.SERIALIZER_HTTP_HEADER,
                               request.getSerializer().getClass().getName() );
        }

        httpPut.setEntity( new ByteArrayEntity( getPutContent( request ) ) );

        try
        {
            HttpResponse response = httpClient.execute( httpPut );
            StatusLine statusLine = response.getStatusLine();
            switch ( statusLine.getStatusCode() )
            {
                case 200:
                    return new DirectMemoryResponse().setStored( Boolean.TRUE );
                case 204:
                    return new DirectMemoryResponse().setStored( Boolean.FALSE );
                default:
                    throw new DirectMemoryException(
                        "put cache content return http code:'" + statusLine.getStatusCode() + "', reasonPhrase:"
                            + statusLine.getReasonPhrase() );

            }

        }
        catch ( IOException e )
        {
            throw new DirectMemoryException( e.getMessage(), e );
        }
    }

    @Override
    public Future<DirectMemoryResponse> asyncPut( final DirectMemoryRequest request )
        throws DirectMemoryException
    {
        return Executors.newSingleThreadExecutor().submit( new Callable<DirectMemoryResponse>()
        {
            @Override
            public DirectMemoryResponse call()
                throws Exception
            {
                return put( request );
            }
        } );
    }

    @Override
    public DirectMemoryResponse get( DirectMemoryRequest request )
        throws DirectMemoryException
    {
        String uri = buildRequestWithKey( request );

        log.debug( "get request to: {}", uri );

        HttpGet httpGet = new HttpGet( uri );

        httpGet.addHeader( "Accept", getAcceptContentType( request ) );

        if ( request.getExchangeType() == ExchangeType.TEXT_PLAIN )
        {
            httpGet.addHeader( DirectMemoryHttpConstants.SERIALIZER_HTTP_HEADER,
                               request.getSerializer().getClass().getName() );
        }
        try
        {
            HttpResponse httpResponse = this.httpClient.execute( httpGet );

            // handle no content response
            StatusLine statusLine = httpResponse.getStatusLine();
            if ( statusLine.getStatusCode() == 204 )
            {
                return new DirectMemoryResponse().setFound( false );
            }

            if ( request.isDeleteRequest() )
            {
                return new DirectMemoryResponse().setFound( true ).setDeleted( true );
            }

            return buildResponse( httpResponse.getEntity().getContent(), request ).setFound( true );
        }
        catch ( IOException e )
        {
            throw new DirectMemoryException( e.getMessage(), e );
        }
    }

    @Override
    public Future<DirectMemoryResponse> asyncGet( final DirectMemoryRequest request )
        throws DirectMemoryException
    {
        return Executors.newSingleThreadExecutor().submit( new Callable<DirectMemoryResponse>()
        {
            @Override
            public DirectMemoryResponse call()
                throws Exception
            {
                return get( request );
            }
        } );
    }

    @Override
    public DirectMemoryResponse delete( DirectMemoryRequest request )
        throws DirectMemoryException
    {
        String uri = buildRequestWithKey( request );

        log.debug( "get request to: {}", uri );

        HttpDelete httpDelete = new HttpDelete( uri );

        try
        {
            HttpResponse httpResponse = this.httpClient.execute( httpDelete );

            // handle no content response
            StatusLine statusLine = httpResponse.getStatusLine();
            if ( statusLine.getStatusCode() == 204 )
            {
                return new DirectMemoryResponse().setFound( false ).setDeleted( false );
            }

            return new DirectMemoryResponse().setFound( true ).setDeleted( true );

        }
        catch ( IOException e )
        {
            throw new DirectMemoryException( e.getMessage(), e );
        }
    }

    @Override
    public Future<DirectMemoryResponse> asyncDelete( final DirectMemoryRequest request )
        throws DirectMemoryException
    {
        return Executors.newSingleThreadExecutor().submit( new Callable<DirectMemoryResponse>()
        {
            @Override
            public DirectMemoryResponse call()
                throws Exception
            {
                return delete( request );
            }
        } );
    }
}
