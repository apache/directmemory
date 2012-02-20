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

import org.apache.directmemory.serialization.Serializer;

/**
 * json format request:
 * {"DirectMemoryRQ":{"key":"101","put":true,"expiresIn":123,
 * "cacheContent":""}}
 * <p/>
 * cache content is byte[] ie object serialisation
 *
 * @author Olivier Lamy
 */
public class DirectMemoryCacheRequest<V>
    extends AbstractDirectMemoryCacheExchange<V>
{
    /**
     * to update/put content in the server
     */
    private boolean update;

    private int expiresIn;

    private ExchangeType exchangeType;

    private Class<V> objectClass;

    /**
     * to generate a delete request <b>key mandatory</b>
     */
    private boolean deleteRequest = false;

    public boolean isUpdate()
    {
        return update;
    }

    public DirectMemoryCacheRequest setUpdate( boolean update )
    {
        this.update = update;
        return this;
    }

    public int getExpiresIn()
    {
        return expiresIn;
    }

    public DirectMemoryCacheRequest setExpiresIn( int expiresIn )
    {
        this.expiresIn = expiresIn;
        return this;
    }

    public DirectMemoryCacheRequest setKey( String key )
    {
        super.setKey( key );
        return this;
    }


    public DirectMemoryCacheRequest setObject( V object )
    {
        super.setObject( object );
        return this;
    }


    public DirectMemoryCacheRequest setSerializer( Serializer serializer )
    {
        super.setSerializer( serializer );
        return this;
    }


    public DirectMemoryCacheRequest setCacheContent( byte[] cacheContent )
    {
        super.setCacheContent( cacheContent );
        return this;
    }

    public boolean isDeleteRequest()
    {
        return deleteRequest;
    }

    public DirectMemoryCacheRequest setDeleteRequest( boolean deleteRequest )
    {
        this.deleteRequest = deleteRequest;
        return this;
    }

    public ExchangeType getExchangeType()
    {
        return exchangeType;
    }

    public DirectMemoryCacheRequest setExchangeType( ExchangeType exchangeType )
    {
        this.exchangeType = exchangeType;
        return this;
    }

    public Class<V> getObjectClass()
    {
        return objectClass;
    }

    public DirectMemoryCacheRequest setObjectClass( Class<V> objectClass )
    {
        this.objectClass = objectClass;
        return this;
    }
}
