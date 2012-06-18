package org.apache.directmemory.server.client.providers.asynchttpclient;

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

import com.ning.http.client.AsyncCompletionHandler;
import com.ning.http.client.Response;
import org.apache.directmemory.server.commons.DirectMemoryException;
import org.apache.directmemory.server.commons.DirectMemoryHttpConstants;
import org.apache.directmemory.server.commons.DirectMemoryRequest;
import org.apache.directmemory.server.commons.DirectMemoryResponse;

/**
 * @author Olivier Lamy
 */
public class DirectMemoryPutHandler
    extends AsyncCompletionHandler<DirectMemoryResponse>
{
    private DirectMemoryRequest request;

    public DirectMemoryPutHandler( DirectMemoryRequest request )
    {
        this.request = request;
    }

    @Override
    public DirectMemoryResponse onCompleted( Response response )
        throws Exception
    {
        int statusCode = response.getStatusCode();
        switch ( statusCode )
        {
            case 200:
                String headerValue = response.getHeader( DirectMemoryHttpConstants.EXPIRES_SERIALIZE_SIZE );
                int storedSize = headerValue == null ? -1 : Integer.valueOf( headerValue );
                return new DirectMemoryResponse().setStored( Boolean.TRUE ).setStoredSize( storedSize );
            case 204:
                return new DirectMemoryResponse().setStored( Boolean.FALSE );
            default:
                throw new DirectMemoryException(
                    "put cache content return http code:'" + statusCode + "', reasonPhrase:"
                        + response.getStatusText() );
        }
    }
}
