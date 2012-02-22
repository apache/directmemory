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
import org.apache.directmemory.server.commons.DirectMemoryException;
import org.apache.directmemory.server.commons.DirectMemoryRequest;
import org.apache.directmemory.server.commons.DirectMemoryResponse;

import java.io.IOException;
import java.util.concurrent.Future;

/**
 * @author Olivier Lamy
 */
public class DefaultDirectMemoryClient
    implements DirectMemoryClient
{

    public static DirectMemoryClient instance( DirectMemoryClientConfiguration configuration )
        throws DirectMemoryException
    {
        return new DefaultDirectMemoryClient( configuration );
    }

    private DirectMemoryClientConfiguration clientConfiguration;

    private DirectMemoryHttpClient directMemoryHttpClient;

    private DefaultDirectMemoryClient( DirectMemoryClientConfiguration configuration )
        throws DirectMemoryException
    {
        this.directMemoryHttpClient = configuration.getDirectMemoryHttpClient();
        this.clientConfiguration = configuration;
    }


    @Override
    public DirectMemoryResponse retrieve( DirectMemoryRequest directMemoryRequest )
        throws DirectMemoryException, IOException, ClassNotFoundException, InstantiationException,
        IllegalAccessException
    {
        verifyPerRequestParameters( directMemoryRequest );
        DirectMemoryResponse response = this.directMemoryHttpClient.get( directMemoryRequest );
        if ( response.isFound() && response.getCacheContent() != null && response.getCacheContent().length > 0 )
        {
            Serializer serializer = directMemoryRequest.getSerializer();
            if ( serializer == null )
            {
                serializer = clientConfiguration.getSerializer();
            }
            response.setResponse(
                serializer.deserialize( response.getCacheContent(), directMemoryRequest.getObjectClass() ) );
        }
        return response;
    }

    @Override
    public Future<DirectMemoryResponse> asyncRetrieve( DirectMemoryRequest directMemoryRequest )
        throws DirectMemoryException
    {
        verifyPerRequestParameters( directMemoryRequest );
        return this.directMemoryHttpClient.asyncGet( directMemoryRequest );
    }

    @Override
    public void put( DirectMemoryRequest directMemoryRequest )
        throws DirectMemoryException
    {
        verifyPerRequestParameters( directMemoryRequest );
        this.directMemoryHttpClient.put( directMemoryRequest );
    }

    @Override
    public Future<Void> asyncPut( DirectMemoryRequest directMemoryRequest )
        throws DirectMemoryException
    {
        verifyPerRequestParameters( directMemoryRequest );
        return this.directMemoryHttpClient.asyncPut( directMemoryRequest );
    }

    @Override
    public DirectMemoryResponse delete( DirectMemoryRequest directMemoryRequest )
        throws DirectMemoryException
    {
        verifyPerRequestParameters( directMemoryRequest );
        return this.directMemoryHttpClient.delete( directMemoryRequest.setDeleteRequest( true ) );
    }

    @Override
    public Future<DirectMemoryResponse> asyncDelete( DirectMemoryRequest directMemoryRequest )
        throws DirectMemoryException
    {
        verifyPerRequestParameters( directMemoryRequest );
        return this.directMemoryHttpClient.asyncDelete( directMemoryRequest.setDeleteRequest( true ) );
    }

    private void verifyPerRequestParameters(DirectMemoryRequest request)
    {
        if ( request.getSerializer() == null )
        {
            request.setSerializer( this.clientConfiguration.getSerializer() );
        }
        if ( request.getExchangeType() == null )
        {
            request.setExchangeType( this.clientConfiguration.getExchangeType() );
        }
    }
}
