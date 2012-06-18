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

import org.apache.directmemory.server.commons.DirectMemoryException;
import org.apache.directmemory.server.commons.DirectMemoryParser;
import org.apache.directmemory.server.commons.DirectMemoryRequest;
import org.apache.directmemory.server.commons.DirectMemoryResponse;
import org.apache.directmemory.server.commons.DirectMemoryWriter;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;
import java.io.IOException;

/**
 * @author Olivier Lamy
 */
public class JsonContentTypeHandler
    implements ContentTypeHandler
{

    private DirectMemoryParser parser = DirectMemoryParser.instance();

    private DirectMemoryWriter writer = DirectMemoryWriter.instance();

    @Override
    public byte[] handleGet( DirectMemoryRequest request, byte[] cacheResponseContent, HttpServletResponse resp,
                             HttpServletRequest req )
        throws DirectMemoryException, IOException
    {
        DirectMemoryResponse response =
            new DirectMemoryResponse().setKey( request.getKey() ).setCacheContent( cacheResponseContent );
        String json = writer.generateJsonResponse( response );
        resp.setContentType( MediaType.APPLICATION_JSON );
        return json.getBytes();
    }

    @Override
    public DirectMemoryRequest handlePut( HttpServletRequest req, HttpServletResponse resp )
        throws DirectMemoryException, IOException
    {
        // 	application/json

        DirectMemoryRequest request = parser.buildRequest( req.getInputStream() );
        return request;

    }
}
