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

import com.ning.http.client.AsyncHttpClientConfig;
import org.apache.directmemory.serialization.Serializer;
import org.apache.directmemory.server.commons.ExchangeType;

/**
 * @author Olivier Lamy
 */
public class DirectMemoryClientConfiguration
{


    public static final int DEFAULT_MAX_CONCURRENT_CONNECTION = 20;

    public static final long DEFAULT_CONNECTION_TIME_OUT = 5000;

    public static final long DEFAULT_READ_TIME_OUT = 10000;

    private String protocol = "http";

    private String host;

    private int port = 80;

    private String httpPath;

    private int maxConcurentConnections = DEFAULT_MAX_CONCURRENT_CONNECTION;

    private long connectionTimeOut = DEFAULT_CONNECTION_TIME_OUT;

    private long readTimeOut = DEFAULT_READ_TIME_OUT;

    private ExchangeType exchangeType;

    private Serializer serializer;

    private String httpClientClassName = DirectMemoryClientBuilder.DEFAULT_HTTP_CLIENT_INSTANCE;

    private AsyncHttpClientConfig asyncHttpClientConfig;
    /**
     * http provider class for async http client
     * default value is <code>NettyAsyncHttpProvider</code>
     */
    private String httpProviderClassName = null;

    public DirectMemoryClientConfiguration()
    {
        // no op
    }

    public String getHost()
    {
        return host;
    }

    public DirectMemoryClientConfiguration setHost( String host )
    {
        this.host = host;
        return this;
    }

    public int getPort()
    {
        return port;
    }

    public DirectMemoryClientConfiguration setPort( int port )
    {
        this.port = port;
        return this;
    }

    public String getHttpPath()
    {
        return httpPath;
    }

    public DirectMemoryClientConfiguration setHttpPath( String httpPath )
    {
        this.httpPath = httpPath;
        return this;
    }


    public int getMaxConcurentConnections()
    {
        return maxConcurentConnections;
    }

    public DirectMemoryClientConfiguration setMaxConcurentConnections( int maxConcurentConnections )
    {
        this.maxConcurentConnections = maxConcurentConnections;
        return this;
    }

    public long getConnectionTimeOut()
    {
        return connectionTimeOut;
    }

    public DirectMemoryClientConfiguration setConnectionTimeOut( long connectionTimeOut )
    {
        this.connectionTimeOut = connectionTimeOut;
        return this;
    }

    public long getReadTimeOut()
    {
        return readTimeOut;
    }

    public DirectMemoryClientConfiguration setReadTimeOut( long readTimeOut )
    {
        this.readTimeOut = readTimeOut;
        return this;
    }

    public String getProtocol()
    {
        return protocol;
    }

    public DirectMemoryClientConfiguration setProtocol( String protocol )
    {
        this.protocol = protocol;
        return this;
    }

    public ExchangeType getExchangeType()
    {
        return exchangeType;
    }

    public DirectMemoryClientConfiguration setExchangeType( ExchangeType exchangeType )
    {
        this.exchangeType = exchangeType;
        return this;
    }

    public Serializer getSerializer()
    {
        return serializer;
    }

    public DirectMemoryClientConfiguration setSerializer( Serializer serializer )
    {
        this.serializer = serializer;
        return this;
    }

    public String getHttpClientClassName()
    {
        return httpClientClassName;
    }

    public DirectMemoryClientConfiguration setHttpClientClassName( String httpClientClassName )
    {
        this.httpClientClassName = httpClientClassName;
        return this;
    }

    public String getHttpProviderClassName()
    {
        return httpProviderClassName;
    }

    public DirectMemoryClientConfiguration setHttpProviderClassName( String httpProviderClassName )
    {
        this.httpProviderClassName = httpProviderClassName;
        return this;
    }

    public AsyncHttpClientConfig getAsyncHttpClientConfig()
    {
        return asyncHttpClientConfig;
    }

    public DirectMemoryClientConfiguration setAsyncHttpClientConfig( AsyncHttpClientConfig asyncHttpClientConfig )
    {
        this.asyncHttpClientConfig = asyncHttpClientConfig;
        return this;
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder();
        sb.append( "DirectMemoryClientConfiguration" );
        sb.append( "{protocol='" ).append( protocol ).append( '\'' );
        sb.append( ", host='" ).append( host ).append( '\'' );
        sb.append( ", port=" ).append( port );
        sb.append( ", httpPath='" ).append( httpPath ).append( '\'' );
        sb.append( ", maxConcurentConnections=" ).append( maxConcurentConnections );
        sb.append( ", connectionTimeOut=" ).append( connectionTimeOut );
        sb.append( ", readTimeOut=" ).append( readTimeOut );
        sb.append( ", exchangeType=" ).append( exchangeType );
        sb.append( ", serializer=" ).append( serializer );
        sb.append( ", httpClientClassName='" ).append( httpClientClassName ).append( '\'' );
        sb.append( '}' );
        return sb.toString();
    }
}
