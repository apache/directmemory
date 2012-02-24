package org.apache.directmemory;

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

import static com.google.common.base.Preconditions.checkArgument;

import org.apache.directmemory.cache.CacheService;

public final class DirectMemory
{

    public static <K, V> CacheService<K, V> createNewInstance( CacheConfiguration<K, V> cacheConfiguration )
    {
        checkArgument( cacheConfiguration != null, "Impossible to create a cache from a null configuration" );

        CacheConfiguratorImpl<K, V> cacheConfigurator = new CacheConfiguratorImpl<K, V>();
        cacheConfiguration.configure( cacheConfigurator );
        return cacheConfigurator.createInstance();
    }

    /**
     * This class cannot be instantiated.
     */
    private DirectMemory()
    {
        // does nothing
    }

}
