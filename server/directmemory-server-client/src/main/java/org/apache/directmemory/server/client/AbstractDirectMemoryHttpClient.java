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
import org.apache.directmemory.server.commons.DirectMemoryCacheException;
import org.apache.directmemory.server.commons.DirectMemoryCacheParser;
import org.apache.directmemory.server.commons.DirectMemoryCacheRequest;
import org.apache.directmemory.server.commons.DirectMemoryCacheResponse;
import org.apache.directmemory.server.commons.DirectMemoryCacheWriter;
import org.apache.directmemory.server.commons.ExchangeType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;
import java.io.InputStream;

/**
 * @author Olivier Lamy
 */
public abstract class AbstractDirectMemoryHttpClient
    implements DirectMemoryHttpClient
{

    protected Logger log = LoggerFactory.getLogger( getClass() );

    private DirectMemoryCacheWriter writer = DirectMemoryCacheWriter.instance();

    private DirectMemoryCacheParser parser = DirectMemoryCacheParser.instance();

    protected DirectMemoryClientConfiguration configuration;

    protected AbstractDirectMemoryHttpClient( DirectMemoryClientConfiguration configuration )
    {
        this.configuration = configuration;
    }

    protected byte[] getPutContent( DirectMemoryCacheRequest request )
        throws DirectMemoryCacheException
    {
        // TODO handle various exchange model json raw etc..

        if ( request.getExchangeType() == ExchangeType.JSON )
        {
            return writer.generateJsonRequest( request ).getBytes();
        }
        else if ( request.getExchangeType() == ExchangeType.JAVA_SERIALIZED_OBJECT )
        {
            try
            {
                return request.getSerializer().serialize( request.getObject() );
            }
            catch ( IOException e )
            {
                throw new DirectMemoryCacheException( e.getMessage(), e );
            }
        }
        else if ( request.getExchangeType() == ExchangeType.TEXT_PLAIN )
        {
            log.error( "{} not implemented yet", ExchangeType.TEXT_PLAIN.getContentType() );
            throw new NotImplementedException();//  );
        }
        else
        {
            log.error( "exchange type unknown {}", request.getExchangeType() );
            throw new DirectMemoryCacheException( "exchange type unknown " + request.getExchangeType() );
        }
    }

    protected DirectMemoryCacheResponse buildResponse( InputStream inputStream, DirectMemoryCacheRequest request )
        throws DirectMemoryCacheException
    {

        if ( request.getExchangeType() == ExchangeType.JSON )
        {
            return parser.buildResponse( inputStream );
        }
        else if ( request.getExchangeType() == ExchangeType.JAVA_SERIALIZED_OBJECT )
        {
            try
            {
                DirectMemoryCacheResponse response = new DirectMemoryCacheResponse();
                response.setResponse( request.getSerializer().deserialize( IOUtils.toByteArray( inputStream ),
                                                                           request.getObjectClass() ) );
                return response;

            }
            catch ( IOException e )
            {
                throw new DirectMemoryCacheException( e.getMessage(), e );
            }
            catch ( ClassNotFoundException e )
            {
                throw new DirectMemoryCacheException( e.getMessage(), e );
            }
            catch ( IllegalAccessException e )
            {
                throw new DirectMemoryCacheException( e.getMessage(), e );
            }
            catch ( InstantiationException e )
            {
                throw new DirectMemoryCacheException( e.getMessage(), e );
            }
        }
        else if ( request.getExchangeType() == ExchangeType.TEXT_PLAIN )
        {
            log.error( "{} not implemented yet", ExchangeType.TEXT_PLAIN.getContentType() );
            throw new NotImplementedException();//  );
        }
        else
        {
            log.error( "exchange type unknown {}", request.getExchangeType() );
            throw new DirectMemoryCacheException( "exchange type unknown " + request.getExchangeType() );
        }


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
        return request.getExchangeType() == null
            ? this.configuration.getExchangeType().getContentType()
            : request.getExchangeType().getContentType();
    }

    protected String getAcceptContentType( DirectMemoryCacheRequest request )
    {
        return request.getExchangeType() == null
            ? this.configuration.getExchangeType().getContentType()
            : request.getExchangeType().getContentType();
    }
}
