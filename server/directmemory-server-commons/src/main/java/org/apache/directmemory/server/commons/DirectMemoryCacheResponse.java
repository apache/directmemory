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
 * xml format response:
 * <pre><![CDATA[
 * <DirectMemoryRS version="1.0" found="" updated="true" key="">
 *   <cacheContent>
 *     <![CDATA[
 *     ]]>
 *   </cacheContent>
 * </DirectMemoryRS>]]>
 * </pre>
 *
 * @author Olivier Lamy
 */
public class DirectMemoryCacheResponse
    extends AbstractDirectMemoryCacheExchange
{
    private boolean found;

    private boolean updated;

    private Class objectClass;

    public boolean isFound()
    {
        return found;
    }

    public DirectMemoryCacheResponse setFound( boolean found )
    {
        this.found = found;
        return this;
    }

    public boolean isUpdated()
    {
        return updated;
    }

    public DirectMemoryCacheResponse setUpdated( boolean updated )
    {
        this.updated = updated;
        return this;
    }

    public Class getObjectClass()
    {
        return objectClass;
    }

    public DirectMemoryCacheResponse setObjectClass( Class objectClass )
    {
        this.objectClass = objectClass;
        return this;
    }

    public DirectMemoryCacheResponse setCacheContent( byte[] cacheContent )
    {
        super.setCacheContent( cacheContent );
        return this;
    }

    public DirectMemoryCacheResponse setKey( String key )
    {
        super.setKey( key );
        return this;
    }
}
