package org.apache.directmemory.server.commons;
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
 * json format response:
 * {"DirectMemoryRS":{"found":true,"updated":false,"key":"foo","cacheContent":""}}
 * <p/>
 * cache content is byte[] ie object serialisation
 *
 * @author Olivier Lamy
 */
public class DirectMemoryResponse<V>
    extends AbstractDirectMemoryExchange
{
    private boolean found;

    private boolean updated;

    private boolean deleted = false;

    private V response;

    public boolean isFound()
    {
        return found;
    }

    public DirectMemoryResponse setFound( boolean found )
    {
        this.found = found;
        return this;
    }

    public boolean isUpdated()
    {
        return updated;
    }

    public DirectMemoryResponse setUpdated( boolean updated )
    {
        this.updated = updated;
        return this;
    }

    public DirectMemoryResponse setCacheContent( byte[] cacheContent )
    {
        super.setCacheContent( cacheContent );
        return this;
    }

    public DirectMemoryResponse setKey( String key )
    {
        super.setKey( key );
        return this;
    }

    public boolean isDeleted()
    {
        return deleted;
    }

    public DirectMemoryResponse setDeleted( boolean deleted )
    {
        this.deleted = deleted;
        return this;
    }

    public V getResponse()
    {
        return this.response;
    }

    public void setResponse( V response )
    {
        this.response = response;
    }
}
