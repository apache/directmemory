package org.apache.directmemory.conf;

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
 * @since 0.2
 */
public abstract class DefaultConfiguration
    implements ConfigurationService
{
    private int numberOfBuffers = 1;

    private int initialCapacity = 100000;

    private int ramMegaBytes = 1;

    private int concurrencyLevel = 4;

    private long disposalTime = 10L;

    @Override
    public int getNumberOfBuffers()
    {
        return numberOfBuffers;
    }

    @Override
    public int getInitialCapacity()
    {
        return initialCapacity;
    }

    @Override
    public int getRamMegaBytes()
    {
        return ramMegaBytes;
    }

    @Override
    public int getConcurrencyLevel()
    {
        return concurrencyLevel;
    }

    @Override
    public long getDisposalTime()
    {
        return disposalTime;
    }

    @Override
    public void setNumberOfBuffers( int numberOfBuffers )
    {
        this.numberOfBuffers = numberOfBuffers;
    }

    @Override
    public void setInitialCapacity( int initialCapacity )
    {
        this.initialCapacity = initialCapacity;
    }

    @Override
    public void setRamMegaBytes( int ramMegaBytes )
    {
        this.ramMegaBytes = ramMegaBytes;
    }

    @Override
    public void setConcurrencyLevel( int concurrencyLevel )
    {
        this.concurrencyLevel = concurrencyLevel;
    }

    @Override
    public void setDisposalTime( long disposalTime )
    {
        this.disposalTime = disposalTime;
    }

}
