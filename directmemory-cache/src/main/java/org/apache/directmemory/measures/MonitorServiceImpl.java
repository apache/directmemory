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

package org.apache.directmemory.measures;

import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.directmemory.misc.Format;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MonitorServiceImpl
    implements MonitorService
{

    private AtomicLong hits = new AtomicLong( 0 );

    private long totalTime = 0;

    private long min = -1;

    private long max = -1;

    public String name;

    private static final Logger logger = LoggerFactory.getLogger( MonitorServiceImpl.class );

    public static final Map<String, MonitorServiceImpl> monitors = new HashMap<String, MonitorServiceImpl>();

    public MonitorServiceImpl( String name )
    {
        this.name = name;
    }

    public long start()
    {
        return System.nanoTime();
    }

    public long stop( long begunAt )
    {
        hits.incrementAndGet();
        final long lastAccessed = System.nanoTime();
        final long elapsed = lastAccessed - begunAt;
        totalTime += elapsed;
        if ( elapsed > max && hits.get() > 0 )
        {
            max = elapsed;
        }
        if ( elapsed < min && hits.get() > 0 )
        {
            min = elapsed;
        }
        return elapsed;
    }

    public long hits()
    {
        return hits.get();
    }

    public long totalTime()
    {
        return totalTime;
    }

    public long average()
    {
        return hits.get() > 0 ? totalTime / hits.get() : 0;
    }

    public String toString()
    {
        return Format.it( "%1$s hits: %2$d, avg: %3$s ms, tot: %4$s seconds", name, hits.get(),
                          new DecimalFormat( "####.###" ).format( (double) average() / 1000000 ),
                          new DecimalFormat( "####.###" ).format( (double) totalTime / 1000000000 ) );
    }

    public void dump( String prefix )
    {
        for ( MonitorServiceImpl monitor : MonitorServiceImpl.monitors.values() )
        {
            if ( monitor.name.startsWith( prefix ) )
            {
                logger.info( monitor.toString() );
            }
        }
    }

    public void dump()
    {
        dump( "" );
    }

    public String getName()
    {
        return name;
    }

    public AtomicLong getHits()
    {
        return hits;
    }

    public long getTotalTime()
    {
        return totalTime;
    }

    @Override
    public void addToTotalTime( long time )
    {
        totalTime += time;
    }

    public long getMin()
    {
        return min;
    }

    public void setMin( long min )
    {
        this.min = min;
    }

    public long getMax()
    {
        return max;
    }

    public void setMax( long max )
    {
        this.max = max;
    }


}
