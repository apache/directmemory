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

import org.apache.directmemory.measures.Ram;
import org.apache.directmemory.memory.MemoryManager;
import org.apache.directmemory.memory.allocator.Allocator;
import org.apache.directmemory.memory.allocator.MergingByteBufferAllocatorImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertNotNull;

public class Starter
{

    private static Logger logger = LoggerFactory.getLogger( MallocTest.class );

    /**
     * @param args
     */
    public static void main( String[] args )
    {

        if ( args.length < 3 )
        {
            System.out.println( "DirectMemory (for real testers only!) - usage:" );
            System.out.println(
                "	java -XX:MaxDirectMemorySize=XXXXm -XmxXXXXm -XmsXXXXm -jar dm-test.jar <buffers> <Mb for each buffer> <entries>" );
            return;
        }

        int buffers = new Integer( args[0] );
        int mb = new Integer( args[1] );
        int entries = new Integer( args[2] );

        logger.info( "buffers: " + buffers );
        logger.info( "mb: " + mb );
        logger.info( "entries: " + entries );

        Starter starter = new Starter();
        starter.rawInsertMultipleBuffers( buffers, mb, entries );
    }

    
    private static void dump( MemoryManagerService<Object> mms )
    {
        logger.info( "off-heap - allocated: " + Ram.inMb( mms.capacity() ) );
        logger.info( "off-heap - used:      " + Ram.inMb( mms.used() ) );
        logger.info( "heap    - max: " + Ram.inMb( Runtime.getRuntime().maxMemory() ) );
        logger.info( "heap     - allocated: " + Ram.inMb( Runtime.getRuntime().totalMemory() ) );
        logger.info( "heap     - free : " + Ram.inMb( Runtime.getRuntime().freeMemory() ) );
        logger.info( "************************************************" );
    }
    
    public void dump( Allocator mem )
    {
        logger.info( "off-heap - buffer: " + mem.getNumber() );
        logger.info( "off-heap - allocated: " + Ram.inMb( mem.getCapacity() ) );
        logger.info( "heap    - max: " + Ram.inMb( Runtime.getRuntime().maxMemory() ) );
        logger.info( "heap     - allocated: " + Ram.inMb( Runtime.getRuntime().totalMemory() ) );
        logger.info( "heap     - free : " + Ram.inMb( Runtime.getRuntime().freeMemory() ) );
        logger.info( "************************************************" );
    }
    
    public void rawInsert( int megabytes, int howMany )
    {
        Allocator allocator = new MergingByteBufferAllocatorImpl( 1, megabytes * 1024 * 1024 );
        assertNotNull( allocator );
        int size = allocator.getCapacity() / ( howMany );
        size -= size / 100 * 1;
        logger.info( "payload size=" + size );
        logger.info( "entries=" + howMany );

        logger.info( "starting..." );

        long start = System.currentTimeMillis();

        for ( int i = 0; i < howMany; i++ )
        {
            allocator.allocate( size );
        }

        logger.info( "...done in " + ( System.currentTimeMillis() - start ) + " msecs." );
        logger.info( "---------------------------------" );
        dump( allocator );
    }


    public void rawInsertMultipleBuffers( int buffers, int megabytes, int howMany )
    {
        MemoryManager.init( buffers, Ram.Mb( megabytes ) );
        int size = (int) ( MemoryManager.capacity() / ( howMany ) );
        size -= size / 100 * 1;
        logger.info( "payload size=" + size );
        logger.info( "entries=" + howMany );

        logger.info( "starting..." );

        long start = System.currentTimeMillis();

        byte[] payload = new byte[size];
        for ( int i = 0; i < howMany; i++ )
        {
            MemoryManager.store( payload );
        }

        logger.info( "...done in " + ( System.currentTimeMillis() - start ) + " msecs." );
        logger.info( "---------------------------------" );

        dump( MemoryManager.getMemoryManager() );
    }


}
