package org.apache.directmemory.server.services;

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
import org.apache.commons.lang.StringUtils;
import org.apache.directmemory.serialization.Serializer;
import org.apache.directmemory.serialization.SerializerFactory;
import org.apache.directmemory.serialization.SerializerNotFoundException;
import org.apache.directmemory.server.commons.DirectMemoryException;
import org.apache.directmemory.server.commons.DirectMemoryHttpConstants;
import org.apache.directmemory.server.commons.DirectMemoryRequest;
import org.apache.directmemory.server.commons.DirectMemoryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;
import java.io.IOException;

/**
 * @author Olivier Lamy
 */
public class TextPlainContentTypeHandler
    implements ContentTypeHandler
{
    private Logger log = LoggerFactory.getLogger( getClass() );

    @Override
    public byte[] handleGet( DirectMemoryRequest request, byte[] cacheResponseContent, HttpServletResponse resp,
                             HttpServletRequest req )
        throws DirectMemoryException, IOException
    {

        try
        {
            Serializer serializer = getSerializer( req );
            String res = serializer.deserialize( cacheResponseContent, String.class );
            resp.setContentType( MediaType.TEXT_PLAIN );
            return res.getBytes();
        }
        catch ( SerializerNotFoundException e )
        {
            throw new DirectMemoryException( e.getMessage(), e );
        }
        catch ( ClassNotFoundException e )
        {
            throw new DirectMemoryException( e.getMessage(), e );
        }
        catch ( InstantiationException e )
        {
            throw new DirectMemoryException( e.getMessage(), e );
        }
        catch ( IllegalAccessException e )
        {
            throw new DirectMemoryException( e.getMessage(), e );
        }
    }

    @Override
    public DirectMemoryRequest handlePut( HttpServletRequest req, HttpServletResponse resp )
        throws DirectMemoryException, IOException
    {
        DirectMemoryRequest request = new DirectMemoryRequest();
        try
        {
            Serializer serializer = getSerializer( req );
            request.setCacheContent( serializer.serialize( IOUtils.toString( req.getInputStream() ) ) );
        }
        catch ( SerializerNotFoundException e )
        {
            throw new DirectMemoryException( e.getMessage(), e );
        }

        String expiresInHeader = req.getHeader( DirectMemoryHttpConstants.EXPIRES_IN_HTTP_HEADER );
        int expiresIn = StringUtils.isEmpty( expiresInHeader ) ? 0 : Integer.valueOf( expiresInHeader );
        log.debug( "expiresIn: {} for header value: {}", expiresIn, expiresInHeader );

        return request.setExpiresIn( expiresIn );

    }


    protected Serializer getSerializer( HttpServletRequest req )
        throws SerializerNotFoundException
    {
        String serializerClassName = req.getHeader( DirectMemoryHttpConstants.SERIALIZER_HTTP_HEADER );
        if ( StringUtils.isEmpty( serializerClassName ) )
        {
            return SerializerFactory.createNewSerializer();
        }
        return SerializerFactory.createNewSerializer( serializerClassName );
    }
}
