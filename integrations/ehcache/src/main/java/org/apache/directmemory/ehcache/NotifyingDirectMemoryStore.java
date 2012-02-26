package org.apache.directmemory.ehcache;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import net.sf.ehcache.Ehcache;
import net.sf.ehcache.Element;
import net.sf.ehcache.pool.Pool;
import net.sf.ehcache.pool.PoolableStore;

public class NotifyingDirectMemoryStore
    extends DirectMemoryStore
{

    private final Ehcache cache;

    private NotifyingDirectMemoryStore( Ehcache cache, Pool<PoolableStore> pool )
    {
        super( cache, pool, true );
        this.cache = cache;
    }

    public static NotifyingDirectMemoryStore create( Ehcache cache, Pool<PoolableStore> pool )
    {
        NotifyingDirectMemoryStore store = new NotifyingDirectMemoryStore( cache, pool );
        return store;
    }

    protected boolean evict( Element element )
    {
        Element remove = remove( element.getObjectKey() );
        if ( remove != null )
        {
            this.cache.getCacheEventNotificationService().notifyElementEvicted( element, false );
        }
        return remove != null;
    }

    protected void notifyDirectEviction( Element element )
    {
        this.cache.getCacheEventNotificationService().notifyElementEvicted( element, false );
    }

    public void expireElements()
    {
        for ( Object key : this.getKeys() )
        {
            //expire element check if it is expired, if it is expired remove from cache and return element
            //.iskeyvalid()
            Element element = remove( key );
            if ( element != null )
            {
                this.cache.getCacheEventNotificationService().notifyElementExpiry( element, false );
            }
        }
    }
}
