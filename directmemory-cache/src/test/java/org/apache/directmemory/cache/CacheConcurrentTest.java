package org.apache.directmemory.cache;

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

import com.carrotsearch.junitbenchmarks.AbstractBenchmark;
import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.annotation.AxisRange;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkHistoryChart;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;
import com.carrotsearch.junitbenchmarks.annotation.LabelType;
import org.apache.directmemory.cache.Cache;
import org.apache.directmemory.measures.Monitor;
import org.apache.directmemory.measures.Ram;
import org.apache.directmemory.memory.MemoryManager;
import org.apache.directmemory.memory.Pointer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

@AxisRange( min = 0, max = 1 )
@BenchmarkMethodChart()
@BenchmarkHistoryChart( labelWith = LabelType.CUSTOM_KEY, maxRuns = 5 )
@Ignore
public class CacheConcurrentTest
    extends AbstractBenchmark
{

    private final static int entries = 100000;

    public static AtomicInteger count = new AtomicInteger();

    private static AtomicInteger got = new AtomicInteger();

    private static AtomicInteger missed = new AtomicInteger();

    private static AtomicInteger good = new AtomicInteger();

    private static AtomicInteger bad = new AtomicInteger();

    private static AtomicInteger read = new AtomicInteger();

    private static AtomicInteger disposals = new AtomicInteger();

    @BenchmarkOptions( benchmarkRounds = 10000, warmupRounds = 0, concurrency = 1000 )
    @Test
    public void store()
    {
        final String key = "test-" + count.incrementAndGet();
        put( key );
    }

    @BenchmarkOptions( benchmarkRounds = 500, warmupRounds = 0, concurrency = 10 )
    @Test
    public void storeSomeWithExpiry()
    {
        final String key = "test-" + count.incrementAndGet();
        putWithExpiry( key );
    }

    @BenchmarkOptions( benchmarkRounds = 1000000, warmupRounds = 0, concurrency = 100 )
    @Test
    public void retrieveCatchThemAll()
    {
        String key = "test-" + ( rndGen.nextInt( entries ) + 1 );
        get( key );
    }

    @BenchmarkOptions( benchmarkRounds = 1000000, warmupRounds = 0, concurrency = 100 )
    @Test
    public void retrieveCatchHalfOfThem()
    {
        String key = "test-" + ( rndGen.nextInt( entries * 2 ) + 1 );
        get( key );
    }

    private void get( String key )
    {
        Pointer p = Cache.getPointer( key );
        @SuppressWarnings( "unused" ) byte[] check = Cache.retrieveByteArray( key );
        read.incrementAndGet();
        if ( p != null )
        {
            got.incrementAndGet();
            byte[] payload = MemoryManager.retrieve( p );
            if ( ( new String( payload ) ).startsWith( key ) )
            {
                good.incrementAndGet();
            }
            else
            {
                bad.incrementAndGet();
            }
        }
        else
        {
            missed.incrementAndGet();
        }
    }

    private void put( String key )
    {
        final StringBuilder bldr = new StringBuilder();
        for ( int i = 0; i < 100; i++ )
        {
            bldr.append( key );
        }
        Cache.putByteArray( key, bldr.toString().getBytes() );
    }

    private void putWithExpiry( String key )
    {
        final StringBuilder bldr = new StringBuilder();
        for ( int i = 0; i < 100; i++ )
        {
            bldr.append( key );
        }
        Cache.putByteArray( key, bldr.toString().getBytes(), rndGen.nextInt( 2000 ) );
    }


    @BenchmarkOptions( benchmarkRounds = 50000, warmupRounds = 0, concurrency = 10 )
    @Test
    public void write1Read8AndSomeDisposal()
    {
        String key = "test-" + ( rndGen.nextInt( entries * 2 ) + 1 );

        int what = rndGen.nextInt( 10 );

        switch ( what )
        {
            case 0:
                put( key );
                break;
            case 1:
            case 2:
            case 3:
            case 4:
            case 5:
            case 6:
            case 7:
            case 8:
                get( key );
                break;
            default:
                final int rndVal = rndGen.nextInt( 1000 );
                if ( rndVal > 995 )
                {
                    disposals.incrementAndGet();
                    final long start = System.currentTimeMillis();
                    long howMany = MemoryManager.collectExpired();
                    final long end = System.currentTimeMillis();
                    logger.info( "" + howMany + " disposed in " + ( end - start ) + " milliseconds" );
                }
        }

    }

    @BenchmarkOptions( benchmarkRounds = 1000000, warmupRounds = 0, concurrency = 10 )
    @Test
    public void write3Read7()
    {
        String key = "test-" + ( rndGen.nextInt( entries * 2 ) + 1 );

        int what = rndGen.nextInt( 10 );

        switch ( what )
        {
            case 0:
            case 1:
            case 2:
                put( key );
                break;
            default:
                get( key );
                break;
        }
    }

    @BenchmarkOptions( benchmarkRounds = 1000000, warmupRounds = 0, concurrency = 10 )
    @Test
    public void write1Read9()
    {
        String key = "test-" + ( rndGen.nextInt( entries * 2 ) + 1 );

        int what = rndGen.nextInt( 10 );

        switch ( what )
        {
            case 0:
                put( key );
                break;
            default:
                get( key );
                break;

        }

    }

    Random rndGen = new Random();

    private static Logger logger = LoggerFactory.getLogger( CacheConcurrentTest.class );

    @BeforeClass
    public static void init()
    {
        Cache.init( 1, Ram.Mb( 512 ) );
        Cache.dump();
    }

    @AfterClass
    public static void dump()
    {

        Cache.dump();
        Monitor.dump( "cache" );

        logger.info( "************************************************" );
        logger.info( "entries: " + entries );
        logger.info( "inserted: " + Cache.entries() );
        logger.info( "reads: " + read );
        logger.info( "count: " + count );
        logger.info( "got: " + got );
        logger.info( "missed: " + missed );
        logger.info( "good: " + good );
        logger.info( "bad: " + bad );
        logger.info( "disposals: " + disposals );
        logger.info( "************************************************" );
    }

}
	
	

