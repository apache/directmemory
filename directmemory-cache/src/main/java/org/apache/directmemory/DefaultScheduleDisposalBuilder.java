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

final class DefaultScheduleDisposalBuilder
    extends AbstractChainedBuilder
    implements ScheduleDisposalBuilder
{

    private static final long NEVER = 1L;

    public DefaultScheduleDisposalBuilder( CacheConfiguratorImpl<?, ?> cacheConfigurator )
    {
        super( cacheConfigurator );
    }

    @Override
    public void withoutExpiring()
    {
        cacheConfigurator.scheduleDisposal = NEVER;
    }

    @Override
    public TimeMeasureBuilder every( long time )
    {
        cacheConfigurator.checkInput( time > 0, "{ scheduleDisposal().every( %s ) } takes a not valid input to express a time measure", time );
        return new DefaultTimeMeasureBuilder( cacheConfigurator, time );
    }

}
