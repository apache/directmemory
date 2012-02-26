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

import org.apache.commons.io.IOUtils;
import org.apache.directmemory.server.commons.DirectMemoryException;
import org.apache.directmemory.server.commons.DirectMemoryParser;
import org.apache.directmemory.server.commons.DirectMemoryRequest;
import org.apache.directmemory.server.commons.DirectMemoryResponse;
import org.apache.directmemory.server.commons.DirectMemoryWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

/**
 * @author Olivier Lamy
 */
public abstract class AbstractDirectMemoryHttpClient
    implements DirectMemoryHttpClient
{

    protected Logger log = LoggerFactory.getLogger( getClass() );

    private DirectMemoryWriter writer = DirectMemoryWriter.instance();

    private DirectMemoryParser parser = DirectMemoryParser.instance();

    protected DirectMemoryClientConfiguration configuration;

    protected AbstractDirectMemoryHttpClient( DirectMemoryClientConfiguration configuration )
    {
        this.configuration = configuration;
    }

    protected byte[] getPutContent( DirectMemoryRequest request )
        throws DirectMemoryException
    {

        switch ( request.getExchangeType() )
        {
            case JSON:
                return writer.generateJsonRequest( request ).getBytes();
            case JAVA_SERIALIZED_OBJECT:
                try
                {
                    return request.getSerializer().serialize( request.getObject() );
                }
                catch ( IOException e )
                {
                    throw new DirectMemoryException( e.getMessage(), e );
                }
            case TEXT_PLAIN:
                return request.getObject().toString().getBytes();
            default:
                log.error( "exchange type unknown {}", request.getExchangeType() );
                throw new DirectMemoryException( "exchange type unknown " + request.getExchangeType() );
        }
    }

    protected DirectMemoryResponse buildResponse( InputStream inputStream, DirectMemoryRequest request )
        throws DirectMemoryException
    {

        switch ( request.getExchangeType() )
        {
            case JSON:
                return parser.buildResponse( inputStream );
            case JAVA_SERIALIZED_OBJECT:
                try
                {
                    DirectMemoryResponse response = new DirectMemoryResponse();
                    response.setResponse( request.getSerializer().deserialize( IOUtils.toByteArray( inputStream ),
                                                                               request.getObjectClass() ) );
                    return response;

                }
                catch ( IOException e )
                {
                    throw new DirectMemoryException( e.getMessage(), e );
                }
                catch ( ClassNotFoundException e )
                {
                    throw new DirectMemoryException( e.getMessage(), e );
                }
                catch ( IllegalAccessException e )
                {
                    throw new DirectMemoryException( e.getMessage(), e );
                }
                catch ( InstantiationException e )
                {
                    throw new DirectMemoryException( e.getMessage(), e );
                }
            case TEXT_PLAIN:
                try
                {
                    DirectMemoryResponse<String> response = new DirectMemoryResponse<String>();
                    response.setResponse( IOUtils.toString( inputStream ) );
                    return response;
                }
                catch ( IOException e )
                {
                    throw new DirectMemoryException( e.getMessage(), e );
                }
            default:
                log.error( "exchange type unknown {}", request.getExchangeType() );
                throw new DirectMemoryException( "exchange type unknown " + request.getExchangeType() );
        }


    }

    protected String buildRequestWithKey( DirectMemoryRequest request )
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

    protected String getRequestContentType( DirectMemoryRequest request )
    {
        return request.getExchangeType() == null
            ? this.configuration.getExchangeType().getContentType()
            : request.getExchangeType().getContentType();
    }

    protected String getAcceptContentType( DirectMemoryRequest request )
    {
        return request.getExchangeType() == null
            ? this.configuration.getExchangeType().getContentType()
            : request.getExchangeType().getContentType();
    }
}
