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

import org.apache.directmemory.server.commons.DirectMemoryCacheException;
import org.apache.directmemory.server.commons.DirectMemoryCacheRequest;
import org.apache.directmemory.server.commons.DirectMemoryCacheResponse;

import java.io.IOException;
import java.util.concurrent.Future;

/**
 * @author Olivier Lamy
 */
public class DefaultDirectMemoryServerClient
    implements DirectMemoryServerClient
{

    public static DirectMemoryServerClient instance( DirectMemoryServerClientConfiguration configuration )
        throws DirectMemoryCacheException
    {
        return new DefaultDirectMemoryServerClient( configuration );
    }

    private DirectMemoryServerClientConfiguration clientConfiguration;

    private DirectMemoryHttpClient directMemoryHttpClient;

    private DefaultDirectMemoryServerClient( DirectMemoryServerClientConfiguration configuration )
        throws DirectMemoryCacheException
    {
        this.directMemoryHttpClient = configuration.getDirectMemoryHttpClient();
        this.clientConfiguration = configuration;
    }


    @Override
    public DirectMemoryCacheResponse retrieve( DirectMemoryCacheRequest directMemoryCacheRequest )
        throws DirectMemoryCacheException, IOException, ClassNotFoundException, InstantiationException,
        IllegalAccessException
    {
        DirectMemoryCacheResponse response = this.directMemoryHttpClient.get( directMemoryCacheRequest );
        if ( response.isFound() && response.getCacheContent() != null && response.getCacheContent().length > 0 )
        {
            response.setResponse( directMemoryCacheRequest.getSerializer().deserialize( response.getCacheContent(),
                                                                                        directMemoryCacheRequest.getObjectClass() ) );
        }
        return response;
    }

    @Override
    public Future<DirectMemoryCacheResponse> asyncRetrieve( DirectMemoryCacheRequest directMemoryCacheRequest )
        throws DirectMemoryCacheException
    {
        return this.directMemoryHttpClient.asyncGet( directMemoryCacheRequest );
    }

    @Override
    public void put( DirectMemoryCacheRequest directMemoryCacheRequest )
        throws DirectMemoryCacheException
    {
        this.directMemoryHttpClient.put( directMemoryCacheRequest );
    }

    @Override
    public Future<Void> asyncPut( DirectMemoryCacheRequest directMemoryCacheRequest )
        throws DirectMemoryCacheException
    {
        return this.directMemoryHttpClient.asyncPut( directMemoryCacheRequest );
    }

    @Override
    public DirectMemoryCacheResponse delete( DirectMemoryCacheRequest directMemoryCacheRequest )
        throws DirectMemoryCacheException
    {
        return this.directMemoryHttpClient.delete( directMemoryCacheRequest.setDeleteRequest( true ) );
    }

    @Override
    public Future<DirectMemoryCacheResponse> asyncDelete( DirectMemoryCacheRequest directMemoryCacheRequest )
        throws DirectMemoryCacheException
    {
        return this.directMemoryHttpClient.asyncDelete( directMemoryCacheRequest.setDeleteRequest( true ) );
    }
}
