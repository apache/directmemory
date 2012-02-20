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
 * @author Olivier Lamy
 */
public abstract class AbstractDirectMemoryCacheExchange
{
    private String key;

    private Object object;

    private Serializer serializer;

    protected byte[] cacheContent;

    public String getKey()
    {
        return key;
    }

    public AbstractDirectMemoryCacheExchange setKey( String key )
    {
        this.key = key;
        return this;
    }

    public Object getObject()
    {
        return object;
    }

    public AbstractDirectMemoryCacheExchange setObject( Object object )
    {
        this.object = object;
        return this;
    }

    public Serializer getSerializer()
    {
        return serializer;
    }

    public AbstractDirectMemoryCacheExchange setSerializer( Serializer serializer )
    {
        this.serializer = serializer;
        return this;
    }

    public byte[] getCacheContent()
    {
        return cacheContent;
    }

    public AbstractDirectMemoryCacheExchange setCacheContent( byte[] cacheContent )
    {
        this.cacheContent = cacheContent;
        return this;
    }
}
