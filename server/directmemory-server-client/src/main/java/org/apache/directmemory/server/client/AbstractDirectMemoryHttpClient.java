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
import org.apache.directmemory.server.commons.DirectMemoryCacheParser;
import org.apache.directmemory.server.commons.DirectMemoryCacheRequest;
import org.apache.directmemory.server.commons.DirectMemoryCacheResponse;
import org.apache.directmemory.server.commons.DirectMemoryCacheWriter;

import java.io.InputStream;

/**
 * @author Olivier Lamy
 */
public abstract class AbstractDirectMemoryHttpClient
    implements DirectMemoryHttpClient
{

    private DirectMemoryCacheWriter writer = DirectMemoryCacheWriter.instance();

    private DirectMemoryCacheParser parser = DirectMemoryCacheParser.instance();

    protected DirectMemoryServerClientConfiguration configuration;

    protected AbstractDirectMemoryHttpClient( DirectMemoryServerClientConfiguration configuration )
    {
        this.configuration = configuration;
    }

    protected byte[] getPutContent( DirectMemoryCacheRequest request )
        throws DirectMemoryCacheException
    {
        // TODO handle various exchange model json raw etc..



        if ( request.getSerializer() == null )
        {
            request.setSerializer( this.configuration.getSerializer() );
        }
        return writer.generateJsonRequest( request ).getBytes();
    }

    protected DirectMemoryCacheResponse buildResponse( InputStream inputStream )
        throws DirectMemoryCacheException
    {
        return parser.buildResponse( inputStream );
    }

    protected String buildRequestWithKey( DirectMemoryCacheRequest request )
    {
        StringBuilder uri = new StringBuilder( this.configuration.getProtocol() );
        uri.append( "://" ).append( this.configuration.getHost() );
        uri.append( ':' ).append( this.configuration.getPort() );
        uri.append( '/' ).append( this.configuration.getHttpPath() );
        uri.append( '/' ).append( request.getKey() );
        // we take care of spaces in the key
        // TODO use something more generics to take care of all characters
        return uri.toString().replace( ' ', '+' );
    }

    protected String getRequestContentType( DirectMemoryCacheRequest request )
    {
        return request.getExchangeType().getContentType();
    }

    protected String getAcceptContentType( DirectMemoryCacheRequest request )
    {
        return request.getExchangeType().getContentType();
    }
}
