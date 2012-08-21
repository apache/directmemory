package org.apache.directmemory.measures;

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

import static java.lang.String.format;

import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Monitor
{

    private static final Logger LOG = LoggerFactory.getLogger( Monitor.class );

    //TODO: MONITORS looks like a good candidate to become a private field
    public static final Map<String, MonitorService> MONITORS = new HashMap<String, MonitorService>();

    private MonitorService monitorService;

    public static MonitorService get( String key )
    {
        MonitorService mon = MONITORS.get( key );
        if ( mon == null )
        {
            mon = new MonitorServiceImpl( key );
            MONITORS.put( key, mon );
        }
        return mon;
    }

    public Monitor( String name )
    {
        this.monitorService = new MonitorServiceImpl( name );
        MONITORS.put( name, monitorService );
    }

    public long start()
    {
        return System.nanoTime();
    }

    public long stop( long begunAt )
    {
        monitorService.getHits().incrementAndGet();
        final long lastAccessed = System.nanoTime();
        final long elapsed = lastAccessed - begunAt;
        monitorService.addToTotalTime( elapsed );
        if ( elapsed > monitorService.getMax() && monitorService.getHits().get() > 0 )
        {
            monitorService.setMax( elapsed );
        }
        if ( elapsed < monitorService.getMin() && monitorService.getHits().get() > 0 )
        {
            monitorService.setMin( elapsed );
        }
        return elapsed;
    }

    public long hits()
    {
        return monitorService.getHits().get();
    }

    public long totalTime()
    {
        return monitorService.totalTime();
    }

    public long average()
    {
        return monitorService.getHits().get() > 0 ? monitorService.getTotalTime() / monitorService.getHits().get() : 0;
    }

    public String toString()
    {
        return format( "%1$s hits: %2$d, avg: %3$s ms, tot: %4$s seconds", monitorService.getName(),
                       monitorService.getHits().get(),
                       new DecimalFormat( "####.###" ).format( (double) average() / 1000000 ),
                       new DecimalFormat( "####.###" ).format( (double) monitorService.getTotalTime() / 1000000000 ) );
    }

    public static void dump( String prefix )
    {
        for ( MonitorService monitor : MONITORS.values() )
        {
            if ( monitor.getName().startsWith( prefix ) )
            {
                LOG.info( monitor.toString() );
            }
        }
    }

    public static void dump()
    {
        dump( "" );
    }
}
