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
import org.apache.directmemory.server.commons.DirectMemoryCacheException;
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

    private int maxConcurentConnection = DEFAULT_MAX_CONCURRENT_CONNECTION;

    private long connectionTimeOut;

    private long readTimeOut;

    private ExchangeType exchangeType;

    private Serializer serializer;

    private DirectMemoryHttpClient directMemoryHttpClient;

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

    public DirectMemoryHttpClient getDirectMemoryHttpClient()
    {
        return directMemoryHttpClient;
    }

    public DirectMemoryClientConfiguration setDirectMemoryHttpClient(
        DirectMemoryHttpClient directMemoryHttpClient )
        throws DirectMemoryCacheException
    {
        this.directMemoryHttpClient = directMemoryHttpClient;
        this.directMemoryHttpClient.configure( this );
        return this;
    }

    public int getMaxConcurentConnection()
    {
        return maxConcurentConnection;
    }

    public DirectMemoryClientConfiguration setMaxConcurentConnection( int maxConcurentConnection )
    {
        this.maxConcurentConnection = maxConcurentConnection;
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
}
