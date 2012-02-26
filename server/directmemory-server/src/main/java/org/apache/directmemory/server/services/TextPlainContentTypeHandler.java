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
import org.apache.directmemory.serialization.Serializer;
import org.apache.directmemory.serialization.SerializerFactory;
import org.apache.directmemory.serialization.SerializerNotFoundException;
import org.apache.directmemory.server.commons.DirectMemoryException;
import org.apache.directmemory.server.commons.DirectMemoryHttpConstants;
import org.apache.directmemory.server.commons.DirectMemoryRequest;
import org.apache.directmemory.server.commons.DirectMemoryResponse;

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

    @Override
    public byte[] handleGet( DirectMemoryRequest request, byte[] cacheResponseContent, HttpServletResponse resp,
                             HttpServletRequest req )
        throws DirectMemoryException, IOException
    {
        DirectMemoryResponse response =
            new DirectMemoryResponse().setKey( request.getKey() ).setCacheContent( cacheResponseContent );
        try
        {
            String serializerClassName = req.getHeader( DirectMemoryHttpConstants.SERIALIZER_HTTP_HEADER );
            Serializer serializer = SerializerFactory.createNewSerializer( serializerClassName );
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
        String serializerClassName = req.getHeader( DirectMemoryHttpConstants.SERIALIZER_HTTP_HEADER );
        DirectMemoryRequest request = new DirectMemoryRequest();
        try
        {
            Serializer serializer = SerializerFactory.createNewSerializer( serializerClassName );
            request.setCacheContent( serializer.serialize( IOUtils.toString( req.getInputStream() ) ) );
        }
        catch ( SerializerNotFoundException e )
        {
            throw new DirectMemoryException( e.getMessage(), e );
        }
        return request;

    }
}
