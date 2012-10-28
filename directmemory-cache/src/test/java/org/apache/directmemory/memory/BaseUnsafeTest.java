package org.apache.directmemory.memory;

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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.Map;
import java.util.Random;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.apache.directmemory.measures.Ram;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.junitbenchmarks.AbstractBenchmark;
import com.google.common.collect.Maps;

public class BaseUnsafeTest
    extends AbstractBenchmark
{

    MemoryManagerService<Object> mem;

    @Before
    public void initMMS()
    {
        mem = new UnsafeMemoryManagerServiceImpl<Object>();
        mem.init( 1, 1 * 1024 * 1024 );
    }

    @After
    public void cleanup()
        throws IOException
    {
        if ( mem != null )
        {
            mem.close();
        }
    }

    @Test
    public void smokeTest()
    {
        logger.info( "buffer size=" + mem.capacity() );
        assertNotNull( mem );

        Random rnd = new Random();

        int size = rnd.nextInt( 10 ) * (int) mem.capacity() / 100;

        logger.info( "size=" + size );

        Pointer<Object> p = mem.store( new byte[size] );
        assertNotNull( p );
        assertEquals( size, mem.used() );
        assertEquals( size, p.getSize() );
        mem.free( p );
        assertEquals( 0, mem.used() );
    }

    private static Logger logger = LoggerFactory.getLogger( MallocTest.class );

    final static Map<String, Byte> test = Maps.newHashMap();

    private static int errors;

    public long crc( String str )
    {
        final Checksum checksum = new CRC32();

        final byte bytes[] = str.getBytes();
        checksum.update( bytes, 0, bytes.length );
        return checksum.getValue();
    }

    @BeforeClass
    @AfterClass
    public static void setup()
    {
        // logger.info("off-heap allocated: " + Ram.inMb(mem.capacity()));
        // logger.info("off-heap used:      " + Ram.inMb(mem.used()));
        logger.info( "test - size: " + test.size() );
        logger.info( "test - errors: " + errors );
        logger.info( "heap - max: " + Ram.inMb( Runtime.getRuntime().maxMemory() ) );
        logger.info( "heap - allocated: " + Ram.inMb( Runtime.getRuntime().totalMemory() ) );
        logger.info( "heap - free : " + Ram.inMb( Runtime.getRuntime().freeMemory() ) );
        logger.info( "************************************************" );
    }

    @Test
    public void aFewEntriesWithRead()
    {
        // logger.info( "total capacity=" + Ram.inMb( mem.capacity() ) );
        assertNotNull( mem );
        int howMany = 100000;
        // logger.info( "payload size is variable" );
        // logger.info( "entries=" + howMany );
        // String test = "this is a nicely crafted test";
        for ( int i = 0; i < howMany; i++ )
        {
            final byte[] payload = ( test + " - " + i ).getBytes();
            Pointer<Object> p = mem.store( payload );
            final byte[] check = mem.retrieve( p );
            assertNotNull( check );
            assertEquals( test + " - " + i, new String( check ) );
            long crc1 = crc32( payload );
            long crc2 = crc32( check );
            assertEquals( crc1, crc2 );
        }

        // logger.info( "total used=" + Ram.inMb( mem.used() ) );
    }

    private static long crc32( byte[] payload )
    {
        final Checksum checksum = new CRC32();
        checksum.update( payload, 0, payload.length );
        return checksum.getValue();
    }

    @Test
    public void aFewEntriesWithCheck()
    {
        // logger.info( "total capacity=" + Ram.inMb( mem.capacity() ) );
        assertNotNull( mem );
        int howMany = 10;
        // logger.info( "payload size is variable" );
        // logger.info( "entries=" + howMany );
        // String test = "this is a nicely crafted test";
        // Pointer<Object> lastP = null;
        for ( int i = 0; i < howMany; i++ )
        {
            byte[] payload = ( test + " - " + i ).getBytes();
            Pointer<Object> p = mem.store( payload );
            // logger.info( "p.start=" + p.getStart() );
            // logger.info( "p.end=" + p.getEnd() );
            assertEquals( p.getCapacity(), payload.length );
            // lastP = p;
            // logger.info( "---" );
        }

        // logger.info( "total used=" + Ram.inMb( mem.used() ) );
    }
}
