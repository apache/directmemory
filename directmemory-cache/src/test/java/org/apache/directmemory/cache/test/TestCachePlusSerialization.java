package org.apache.directmemory.cache.test;

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
import org.apache.directmemory.cache.Cache;
import org.apache.directmemory.measures.Monitor;
import org.apache.directmemory.measures.Ram;
import org.apache.directmemory.misc.DummyPojo;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

import static org.junit.Assert.assertEquals;


@Ignore
public class TestCachePlusSerialization
    extends AbstractBenchmark
{

    private static Logger logger = LoggerFactory.getLogger( TestCachePlusSerialization.class );

    Random rnd = new Random();

    @BeforeClass
    public static void init()
    {
        logger.info( "test started" );
        Cache.init( 1, Ram.Mb( 100 ) );
    }

    @AfterClass
    public static void end()
    {
        Cache.dump();
        Monitor.dump();
        logger.info( "test ended" );
    }

    @BenchmarkOptions( benchmarkRounds = 50000, warmupRounds = 0, concurrency = 1 )
    @Test
    public void basicBench()
    {

        DummyPojo d = new DummyPojo( "test-" + rnd.nextInt( 100000 ), 1024 + rnd.nextInt( 1024 ) );
        Cache.put( d.name, d );
        DummyPojo d2 = (DummyPojo) Cache.retrieve( d.name );

        assertEquals( d.name, d2.name );

    }

}
