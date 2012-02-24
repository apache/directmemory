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

public abstract class AbstractCacheConfiguration<K, V>
    implements CacheConfiguration<K, V>
{

    private CacheConfigurator<K, V> cacheConfigurator;

    /**
     * {@inheritDoc}
     */
    @Override
    public final void configure( CacheConfigurator<K, V> cacheConfigurator )
    {
        if ( this.cacheConfigurator != null )
        {
            throw new IllegalStateException( "Re-entry is not allowed!" );
        }

        this.cacheConfigurator = cacheConfigurator;

        try
        {
            configure();
        }
        finally
        {
            this.cacheConfigurator = null;
        }
    }

    protected abstract void configure();

    protected final MemoryUnitDimensionBuilder allocateMemoryOfSize( double size )
    {
        return cacheConfigurator.allocateMemoryOfSize( size );
    }

    protected final SizeBuilder numberOfBuffers()
    {
        return cacheConfigurator.numberOfBuffers();
    }

    protected final ScheduleDisposalBuilder scheduleDisposal()
    {
        return cacheConfigurator.scheduleDisposal();
    }

}
