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

import com.carrotsearch.junitbenchmarks.AbstractBenchmark;
import com.google.common.collect.Maps;
import org.apache.directmemory.measures.Ram;
import org.apache.directmemory.memory.MemoryManager;
import org.apache.directmemory.memory.OffHeapMemoryBuffer;
import org.apache.directmemory.memory.OffHeapMemoryBufferImpl;
import org.apache.directmemory.memory.Pointer;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class MemoryManagerTest
    extends AbstractBenchmark
{


    @BeforeClass
    public static void init()
    {
        logger.info( "init" );
        MemoryManager.init( 1, Ram.Mb( 100 ) );
    }

    @Test
    public void smokeTest()
    {
        Random rnd = new Random();
        int size = rnd.nextInt( 10 ) * (int) MemoryManager.capacity() / 100;
        logger.info( "payload size=" + Ram.inKb( size ) );
        Pointer<Object> p = MemoryManager.store( new byte[size] );
        logger.info( "stored" );
        assertNotNull( p );
        //assertEquals(size,p.end);
        assertEquals( size, p.getCapacity() );
        assertEquals( size, MemoryManager.getActiveBuffer().used() );
        MemoryManager.free( p );
        assertEquals( 0, MemoryManager.getActiveBuffer().used() );
        logger.info( "end" );
    }

    byte[] payload = "012345678901234567890123456789012345678901234567890123456789".getBytes();

    @Test
    public void fillupTest()
    {
        MemoryManager.clear();
        logger.info( "payload size=" + Ram.inKb( payload.length ) );
        long howMany = ( MemoryManager.capacity() / payload.length );
        howMany = ( howMany * 90 ) / 100;

        for ( int i = 0; i < howMany; i++ )
        {
            Pointer<Object> p = MemoryManager.store( payload );
            assertNotNull( p );
        }

        logger.info( "" + howMany + " items stored" );
    }


    @Test
    public void readTest()
    {
        for ( OffHeapMemoryBuffer<Object> buffer : MemoryManager.getBuffers() )
        {
            for ( Pointer<Object> ptr : ((OffHeapMemoryBufferImpl<Object>) buffer).getPointers() )
            {
                if ( !ptr.free )
                {
                    byte[] res = MemoryManager.retrieve( ptr );
                    assertNotNull( res );
                    assertEquals( new String( payload ), new String( res ) );
                }
            }
        }
    }


    private static Logger logger = LoggerFactory.getLogger( MallocTest.class );

    final static Map<String, Byte> test = Maps.newHashMap();

}
