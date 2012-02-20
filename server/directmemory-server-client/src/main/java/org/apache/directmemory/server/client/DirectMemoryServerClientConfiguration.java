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

/**
 * @author Olivier Lamy
 */
public class DirectMemoryServerClientConfiguration
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

    private DirectMemoryHttpClient directMemoryHttpClient;

    public DirectMemoryServerClientConfiguration()
    {
        // no op
    }

    public String getHost()
    {
        return host;
    }

    public DirectMemoryServerClientConfiguration setHost( String host )
    {
        this.host = host;
        return this;
    }

    public int getPort()
    {
        return port;
    }

    public DirectMemoryServerClientConfiguration setPort( int port )
    {
        this.port = port;
        return this;
    }

    public String getHttpPath()
    {
        return httpPath;
    }

    public DirectMemoryServerClientConfiguration setHttpPath( String httpPath )
    {
        this.httpPath = httpPath;
        return this;
    }

    public DirectMemoryHttpClient getDirectMemoryHttpClient()
    {
        return directMemoryHttpClient;
    }

    public DirectMemoryServerClientConfiguration setDirectMemoryHttpClient(
        DirectMemoryHttpClient directMemoryHttpClient )
    {
        this.directMemoryHttpClient = directMemoryHttpClient;
        return this;
    }

    public int getMaxConcurentConnection()
    {
        return maxConcurentConnection;
    }

    public DirectMemoryServerClientConfiguration setMaxConcurentConnection( int maxConcurentConnection )
    {
        this.maxConcurentConnection = maxConcurentConnection;
        return this;
    }

    public long getConnectionTimeOut()
    {
        return connectionTimeOut;
    }

    public DirectMemoryServerClientConfiguration setConnectionTimeOut( long connectionTimeOut )
    {
        this.connectionTimeOut = connectionTimeOut;
        return this;
    }

    public long getReadTimeOut()
    {
        return readTimeOut;
    }

    public DirectMemoryServerClientConfiguration setReadTimeOut( long readTimeOut )
    {
        this.readTimeOut = readTimeOut;
        return this;
    }

    public String getProtocol()
    {
        return protocol;
    }

    public DirectMemoryServerClientConfiguration setProtocol( String protocol )
    {
        this.protocol = protocol;
        return this;
    }
}
