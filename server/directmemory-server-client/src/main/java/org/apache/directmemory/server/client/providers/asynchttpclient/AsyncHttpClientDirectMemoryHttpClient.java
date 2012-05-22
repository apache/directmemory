package org.apache.directmemory.server.client.providers.asynchttpclient;
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

import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.AsyncHttpClientConfig;
import com.ning.http.client.ListenableFuture;
import com.ning.http.client.providers.netty.NettyAsyncHttpProvider;
import org.apache.directmemory.server.client.AbstractDirectMemoryHttpClient;
import org.apache.directmemory.server.client.DirectMemoryClientConfiguration;
import org.apache.directmemory.server.client.DirectMemoryHttpClient;
import org.apache.directmemory.server.commons.DirectMemoryException;
import org.apache.directmemory.server.commons.DirectMemoryHttpConstants;
import org.apache.directmemory.server.commons.DirectMemoryRequest;
import org.apache.directmemory.server.commons.DirectMemoryResponse;
import org.apache.directmemory.server.commons.ExchangeType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author Olivier Lamy
 */
public class AsyncHttpClientDirectMemoryHttpClient
    extends AbstractDirectMemoryHttpClient
    implements DirectMemoryHttpClient
{
    private Logger log = LoggerFactory.getLogger( getClass() );

    private AsyncHttpClient asyncHttpClient;

    public AsyncHttpClientDirectMemoryHttpClient( DirectMemoryClientConfiguration configuration )
    {
        super( configuration );
        // String providerClass, AsyncHttpClientConfig config
        AsyncHttpClientConfig.Builder builder = new AsyncHttpClientConfig.Builder();
        builder.setConnectionTimeoutInMs( (int) configuration.getConnectionTimeOut() );
        builder.setMaximumConnectionsTotal( configuration.getMaxConcurentConnections() );

        String httpProviderClassName = configuration.getHttpProviderClassName();

        asyncHttpClient = new AsyncHttpClient(
            httpProviderClassName != null ? httpProviderClassName : NettyAsyncHttpProvider.class.getName(),
            configuration.getAsyncHttpClientConfig() == null
                ? builder.build()
                : configuration.getAsyncHttpClientConfig() );
    }

    @Override
    public DirectMemoryResponse put( DirectMemoryRequest request )
        throws DirectMemoryException
    {

        try
        {
            return internalPut( request ).get( configuration.getReadTimeOut(), TimeUnit.MILLISECONDS );
        }
        catch ( InterruptedException e )
        {
            throw new DirectMemoryException( e.getMessage(), e );
        }
        catch ( TimeoutException e )
        {
            throw new DirectMemoryException( e.getMessage(), e );
        }
        catch ( ExecutionException e )
        {
            throw new DirectMemoryException( e.getMessage(), e );
        }

    }

    @Override
    public Future<DirectMemoryResponse> asyncPut( DirectMemoryRequest request )
        throws DirectMemoryException
    {
        return internalPut( request );

    }

    public ListenableFuture<DirectMemoryResponse> internalPut( DirectMemoryRequest request )
        throws DirectMemoryException
    {
        String uri = buildRequestWithKey( request );
        log.debug( "put request to: {}", uri );
        AsyncHttpClient.BoundRequestBuilder requestBuilder = this.asyncHttpClient.preparePut( uri );
        requestBuilder.addHeader( "Content-Type", getRequestContentType( request ) );

        if ( request.getExpiresIn() > 0 )
        {
            requestBuilder.addHeader( DirectMemoryHttpConstants.EXPIRES_IN_HTTP_HEADER,
                                      Integer.toString( request.getExpiresIn() ) );
        }

        if ( request.getExchangeType() == ExchangeType.TEXT_PLAIN )
        {
            requestBuilder.addHeader( DirectMemoryHttpConstants.SERIALIZER_HTTP_HEADER,
                                      request.getSerializer().getClass().getName() );
        }

        requestBuilder.setBody( getPutContent( request ) );
        try
        {
            return asyncHttpClient.executeRequest( requestBuilder.build(), new DirectMemoryPutHandler( request ) );
        }
        catch ( IOException e )
        {
            throw new DirectMemoryException( e.getMessage(), e );
        }

    }

    @Override
    public DirectMemoryResponse get( DirectMemoryRequest request )
        throws DirectMemoryException
    {
        try
        {
            return internalGet( request ).get( configuration.getReadTimeOut(), TimeUnit.MILLISECONDS );
        }
        catch ( InterruptedException e )
        {
            throw new DirectMemoryException( e.getMessage(), e );
        }
        catch ( TimeoutException e )
        {
            throw new DirectMemoryException( e.getMessage(), e );
        }
        catch ( ExecutionException e )
        {
            throw new DirectMemoryException( e.getMessage(), e );
        }
    }

    @Override
    public Future<DirectMemoryResponse> asyncGet( DirectMemoryRequest request )
        throws DirectMemoryException
    {
        return internalGet( request );
    }

    public ListenableFuture<DirectMemoryResponse> internalGet( DirectMemoryRequest request )
        throws DirectMemoryException
    {
        String uri = buildRequestWithKey( request );
        log.debug( "get request to: {}", uri );

        AsyncHttpClient.BoundRequestBuilder requestBuilder = this.asyncHttpClient.prepareGet( uri );
        requestBuilder.addHeader( "Accept", getAcceptContentType( request ) );

        if ( request.getExchangeType() == ExchangeType.TEXT_PLAIN )
        {
            requestBuilder.addHeader( DirectMemoryHttpConstants.SERIALIZER_HTTP_HEADER,
                                      request.getSerializer().getClass().getName() );
        }
        try
        {
            return asyncHttpClient.executeRequest( requestBuilder.build(), new DirectMemoryGetHandler( request ) );
        }
        catch ( IOException e )
        {
            throw new DirectMemoryException( e.getMessage(), e );
        }
    }

    @Override
    public DirectMemoryResponse delete( DirectMemoryRequest request )
        throws DirectMemoryException
    {
        try
        {
            return internalDelete( request ).get( this.configuration.getReadTimeOut(), TimeUnit.MILLISECONDS );
        }
        catch ( InterruptedException e )
        {
            throw new DirectMemoryException( e.getMessage(), e );
        }
        catch ( TimeoutException e )
        {
            throw new DirectMemoryException( e.getMessage(), e );
        }
        catch ( ExecutionException e )
        {
            throw new DirectMemoryException( e.getMessage(), e );
        }
    }

    @Override
    public Future<DirectMemoryResponse> asyncDelete( DirectMemoryRequest request )
        throws DirectMemoryException
    {
        return internalDelete( request );
    }

    public ListenableFuture<DirectMemoryResponse> internalDelete( DirectMemoryRequest request )
        throws DirectMemoryException
    {
        String uri = buildRequestWithKey( request );
        log.debug( "get request to: {}", uri );

        AsyncHttpClient.BoundRequestBuilder requestBuilder = this.asyncHttpClient.prepareGet( uri );
        requestBuilder.addHeader( "Accept", getAcceptContentType( request ) );

        if ( request.getExchangeType() == ExchangeType.TEXT_PLAIN )
        {
            requestBuilder.addHeader( DirectMemoryHttpConstants.SERIALIZER_HTTP_HEADER,
                                      request.getSerializer().getClass().getName() );
        }
        try
        {
            return asyncHttpClient.executeRequest( requestBuilder.build(), new DirectMemoryGetHandler( request ) );
        }
        catch ( IOException e )
        {
            throw new DirectMemoryException( e.getMessage(), e );
        }
    }


}
