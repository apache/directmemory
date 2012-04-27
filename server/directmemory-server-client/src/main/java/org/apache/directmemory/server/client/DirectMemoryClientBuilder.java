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

import org.apache.directmemory.serialization.Serializer;
import org.apache.directmemory.server.client.providers.httpclient.HttpClientDirectMemoryHttpClient;
import org.apache.directmemory.server.commons.DirectMemoryException;
import org.apache.directmemory.server.commons.ExchangeType;

/**
 * @author Olivier Lamy
 */
public class DirectMemoryClientBuilder
{

    public static final String DEFAULT_HTTP_CLIENT_INSTANCE = HttpClientDirectMemoryHttpClient.class.getName();

    private DirectMemoryClientConfiguration configuration;

    protected DirectMemoryClientBuilder()
    {
        this.configuration = new DirectMemoryClientConfiguration();
    }

    protected DirectMemoryClientBuilder( DirectMemoryClientConfiguration configuration )
    {
        this.configuration = configuration;
    }

    public static DirectMemoryClientBuilder newBuilder()
    {
        return new DirectMemoryClientBuilder();
    }

    public static DirectMemoryClientBuilder newBuilder( DirectMemoryClientConfiguration configuration )
    {
        return new DirectMemoryClientBuilder( configuration );
    }

    public DirectMemoryClientBuilder toHost( String host )
    {
        this.configuration.setHost( host );
        return this;
    }

    public DirectMemoryClientBuilder onPort( int port )
    {
        this.configuration.setPort( port );
        return this;
    }

    public DirectMemoryClientBuilder toHttpPath( String HttpPath )
    {
        this.configuration.setHttpPath( HttpPath );
        return this;
    }

    public DirectMemoryClientBuilder withMaxConcurentConnections( int maxConcurentConnections )
    {
        this.configuration.setMaxConcurentConnections( maxConcurentConnections );
        return this;
    }

    public DirectMemoryClientBuilder withConnectionTimeOut( long connectionTimeOut )
    {
        this.configuration.setConnectionTimeOut( connectionTimeOut );
        return this;
    }

    public DirectMemoryClientBuilder withReadTimeOut( long readTimeOut )
    {
        this.configuration.setReadTimeOut( readTimeOut );
        return this;
    }


    public DirectMemoryClientBuilder forExchangeType( ExchangeType exchangeType )
    {
        this.configuration.setExchangeType( exchangeType );
        return this;
    }

    public DirectMemoryClientBuilder withSerializer( Serializer serializer )
    {
        this.configuration.setSerializer( serializer );
        return this;
    }

    public DirectMemoryClientBuilder withHttpClientClassName( String HttpClientClassName )
    {
        this.configuration.setHttpClientClassName( HttpClientClassName );
        return this;
    }

    public DirectMemoryClient buildClient()
        throws DirectMemoryException
    {
        // TODO check here if the builder has received all necessary parameters !
        return new DefaultDirectMemoryClient( this.configuration, buildDirectMemoryHttpClient() );
    }


    protected DirectMemoryHttpClient buildDirectMemoryHttpClient()
    {
        try
        {
            Class<DirectMemoryHttpClient> clientClass =
                (Class<DirectMemoryHttpClient>) Thread.currentThread().getContextClassLoader().loadClass(
                    this.configuration.getHttpClientClassName() );
            return clientClass.getDeclaredConstructor(
                new Class[]{ DirectMemoryClientConfiguration.class } ).newInstance(
                new Object[]{ this.configuration } );
        }
        catch ( Throwable t1 )
        {
            try
            {
                // we try with an other class
                Class<DirectMemoryHttpClient> clientClass =
                    (Class<DirectMemoryHttpClient>) DirectMemoryHttpClient.class.getClassLoader().loadClass(
                        this.configuration.getHttpClientClassName() );
                return clientClass.getDeclaredConstructor(
                    new Class[]{ DirectMemoryClientConfiguration.class } ).newInstance(
                    new Object[]{ this.configuration } );
            }
            catch ( Throwable t2 )
            {
                // ignore that
            }

        }
        // if we are here dynamic stuff has sucks !! so we use default client
        return new HttpClientDirectMemoryHttpClient( this.configuration );
    }


}
