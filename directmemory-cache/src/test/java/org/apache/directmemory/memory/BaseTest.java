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

import java.util.Date;
import java.util.Map;
import java.util.Random;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.apache.directmemory.measures.Ram;
import org.apache.directmemory.memory.OffHeapMemoryBuffer;
import org.apache.directmemory.memory.OffHeapMemoryBufferImpl;
import org.apache.directmemory.memory.Pointer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.junitbenchmarks.AbstractBenchmark;
import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.google.common.collect.Maps;

@Ignore
public class BaseTest
    extends AbstractBenchmark
{
    @Test
    public void smokeTest()
    {
        OffHeapMemoryBuffer<Object> mem = OffHeapMemoryBufferImpl.createNew( 1 * 1024 * 1024 );
        logger.info( "buffer size=" + mem.capacity() );
        assertNotNull( mem );

        Random rnd = new Random();

        int size = rnd.nextInt( 10 ) * mem.capacity() / 100;

        logger.info( "size=" + size );

        Pointer<Object> p = mem.store( new byte[size] );
        assertNotNull( p );
        assertEquals( size, p.end );
        assertEquals( size, mem.used() );
        mem.free( p );
        assertEquals( 0, mem.used() );
    }


    private static Logger logger = LoggerFactory.getLogger( MallocTest.class );

    private static Random rnd = new Random();

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
        rnd = new Random();
//		logger.info("off-heap allocated: " + Ram.inMb(mem.capacity()));
//		logger.info("off-heap used:      " + Ram.inMb(mem.used()));
        logger.info( "test - size: " + test.size() );
        logger.info( "test - errors: " + errors );
        logger.info( "heap - max: " + Ram.inMb( Runtime.getRuntime().maxMemory() ) );
        logger.info( "heap - allocated: " + Ram.inMb( Runtime.getRuntime().totalMemory() ) );
        logger.info( "heap - free : " + Ram.inMb( Runtime.getRuntime().freeMemory() ) );
        logger.info( "************************************************" );
    }

    @BenchmarkOptions( benchmarkRounds = 10000, warmupRounds = 0, concurrency = 10 )
    @Test
    public void anyDuplicates()
    {
        String key = "test" + rnd.nextInt( 100000 );
        if ( test.containsKey( key ) )
        {
            logger.error( "key " + key + " has already been used" );
            errors++;
        }
        test.put( key, (byte) 0 );
    }

    @Test
    public void aFewEntriesWithRead()
    {
        OffHeapMemoryBuffer<Object> mem = OffHeapMemoryBufferImpl.createNew( 100 * 1024 * 1024 );
        logger.info( "total capacity=" + Ram.inMb( mem.capacity() ) );
        assertNotNull( mem );
        int howMany = 10000;
        logger.info( "payload size is variable" );
        logger.info( "entries=" + howMany );
        String test = "this is a nicely crafted test";
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

        logger.info( "total used=" + Ram.inMb( mem.used() ) );
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
        OffHeapMemoryBuffer<Object> mem = OffHeapMemoryBufferImpl.createNew( 10 * 1024 * 1024 );
        logger.info( "total capacity=" + Ram.inMb( mem.capacity() ) );
        assertNotNull( mem );
        int howMany = 10;
        logger.info( "payload size is variable" );
        logger.info( "entries=" + howMany );
        String test = "this is a nicely crafted test";
        Pointer<Object> lastP = null;
        for ( int i = 0; i < howMany; i++ )
        {
            byte[] payload = ( test + " - " + i ).getBytes();
            Pointer<Object> p = mem.store( payload );
            logger.info( "p.start=" + p.start );
            logger.info( "p.end=" + p.end );
            if ( lastP != null )
            {
                assertEquals( lastP.end + 1, p.start );
            }
            assertEquals( p.getCapacity(), payload.length );
            lastP = p;
            logger.info( "---" );
        }

        logger.info( "total used=" + Ram.inMb( mem.used() ) );
    }

    @Test
    public void checkExpiration()
        throws InterruptedException
    {
        OffHeapMemoryBuffer<Object> mem = OffHeapMemoryBufferImpl.createNew( 10 * 1024 * 1024 );
        assertNotNull( mem );
        int size = 400;
        int howMany = 5000;

        logger.info( "off-heap capacity=" + Ram.inMb( mem.capacity() ) );
        logger.info( "payload size=" + Ram.inKb( size ) );
        logger.info( "entries=" + howMany );

        byte[] payload = new byte[size];
        for ( int i = 0; i < howMany; i++ )
        {
            mem.store( payload, 2000 );
        }

        assertEquals( size * howMany, mem.used() );

        logger.info( "entries with relative expiration=" + ( howMany / 2 ) );
        for ( int i = 0; i < howMany / 2; i++ )
        {
            mem.store( payload, 100 );
        }
        assertEquals( size * howMany + size * howMany / 2, mem.used() );

        logger.info( "entries with absolute expiration=" + ( howMany / 2 ) );
        for ( int i = 0; i < howMany / 2; i++ )
        {
            mem.store( payload, new Date() );
        }
        assertEquals( size * howMany * 2, mem.used() );
        logger.info( "total used=" + Ram.inMb( mem.used() ) );

        Thread.sleep( 500 );

        logger.info( "calling disposeExpiredAbsolute" );
        mem.disposeExpiredAbsolute();
        logger.info( "total used=" + Ram.inMb( mem.used() ) );
        assertEquals( size * howMany + size * howMany / 2, mem.used() );

        logger.info( "calling disposeExpiredRelative" );
        mem.disposeExpiredRelative();
        logger.info( "total used=" + Ram.inMb( mem.used() ) );
        assertEquals( size * howMany, mem.used() );

        Thread.sleep( 2000 );

        logger.info( "calling disposeExpiredRelative again" );
        mem.disposeExpiredRelative();
        logger.info( "total used=" + Ram.inMb( mem.used() ) );
        assertEquals( 0, mem.used() );

    }
}
