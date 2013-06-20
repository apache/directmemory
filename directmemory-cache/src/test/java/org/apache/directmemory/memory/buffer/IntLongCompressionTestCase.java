package org.apache.directmemory.memory.buffer;

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

import static org.apache.directmemory.memory.buffer.Int32Compressor.INT32_FULL;
import static org.apache.directmemory.memory.buffer.Int32Compressor.INT32_MAX_DOUBLE;
import static org.apache.directmemory.memory.buffer.Int32Compressor.INT32_MAX_SINGLE;
import static org.apache.directmemory.memory.buffer.Int32Compressor.INT32_MAX_TRIPPLE;
import static org.apache.directmemory.memory.buffer.Int32Compressor.INT32_MIN_DOUBLE;
import static org.apache.directmemory.memory.buffer.Int32Compressor.INT32_MIN_SINGLE;
import static org.apache.directmemory.memory.buffer.Int32Compressor.INT32_MIN_TRIPPLE;
import static org.apache.directmemory.memory.buffer.Int64Compressor.INT64_FULL;
import static org.apache.directmemory.memory.buffer.Int64Compressor.INT64_MAX_DOUBLE;
import static org.apache.directmemory.memory.buffer.Int64Compressor.INT64_MAX_FIFTH;
import static org.apache.directmemory.memory.buffer.Int64Compressor.INT64_MAX_QUAD;
import static org.apache.directmemory.memory.buffer.Int64Compressor.INT64_MAX_SEVENTH;
import static org.apache.directmemory.memory.buffer.Int64Compressor.INT64_MAX_SINGLE;
import static org.apache.directmemory.memory.buffer.Int64Compressor.INT64_MAX_SIXTH;
import static org.apache.directmemory.memory.buffer.Int64Compressor.INT64_MAX_TRIPPLE;
import static org.apache.directmemory.memory.buffer.Int64Compressor.INT64_MIN_DOUBLE;
import static org.apache.directmemory.memory.buffer.Int64Compressor.INT64_MIN_FIFTH;
import static org.apache.directmemory.memory.buffer.Int64Compressor.INT64_MIN_QUAD;
import static org.apache.directmemory.memory.buffer.Int64Compressor.INT64_MIN_SEVENTH;
import static org.apache.directmemory.memory.buffer.Int64Compressor.INT64_MIN_SINGLE;
import static org.apache.directmemory.memory.buffer.Int64Compressor.INT64_MIN_SIXTH;
import static org.apache.directmemory.memory.buffer.Int64Compressor.INT64_MIN_TRIPPLE;
import static org.junit.Assert.assertEquals;

import java.util.Random;

import org.apache.directmemory.memory.allocator.FixedSizeUnsafeAllocatorImpl;
import org.junit.Test;

public class IntLongCompressionTestCase
{

    private static final int TEST_VALUES_COUNT = 1000000;

    @Test
    public void testInt32Compression()
        throws Exception
    {
        FixedSizeUnsafeAllocatorImpl allocator = new FixedSizeUnsafeAllocatorImpl( 1, 5 );
        try
        {
            for ( int i = 0; i < 4; i++ )
            {
                int[] values = new int[TEST_VALUES_COUNT];

                Random random = new Random( -System.currentTimeMillis() );
                int min = ( 0x1 << ( i * 8 ) );
                int max = ( 0xFF << ( i * 8 ) );
                for ( int o = 0; o < TEST_VALUES_COUNT; o++ )
                {
                    values[o] = (int) ( random.nextDouble() * ( max - min + 1 ) ) + min;
                }

                MemoryBuffer buffer = allocator.allocate( 5 );

                for ( int v = 0; v < TEST_VALUES_COUNT; v++ )
                {
                    int value = values[v];
                    buffer.clear();
                    buffer.writeCompressedInt( value );
                    checkIntLength( value, buffer.writerIndex() );

                    int result = buffer.readCompressedInt();
                    assertEquals( value, result );
                }
                allocator.free( buffer );
            }
        }
        finally
        {
            allocator.close();
        }
    }

    @Test
    public void testInt64Compression()
        throws Exception
    {

        FixedSizeUnsafeAllocatorImpl allocator = new FixedSizeUnsafeAllocatorImpl( 1, 9 );
        try
        {
            for ( int i = 0; i < 8; i++ )
            {
                long[] values = new long[TEST_VALUES_COUNT];

                Random random = new Random( -System.currentTimeMillis() );
                int min = ( 0x1 << ( i * 8 ) );
                int max = ( 0xFF << ( i * 8 ) );
                for ( int o = 0; o < TEST_VALUES_COUNT; o++ )
                {
                    values[o] = (long) ( random.nextDouble() * ( max - min + 1 ) ) + min;
                }

                MemoryBuffer buffer = allocator.allocate( 5 );

                for ( int v = 0; v < TEST_VALUES_COUNT; v++ )
                {
                    long value = values[v];
                    buffer.clear();
                    buffer.writeCompressedLong( value );
                    checkLongLength( value, buffer.writerIndex() );

                    long result = buffer.readCompressedLong();
                    assertEquals( value, result );
                }
                allocator.free( buffer );
            }
        }
        finally
        {
            allocator.close();
        }
    }

    private void checkIntLength( int value, long delta )
    {
        if ( value >= INT32_MIN_SINGLE && value <= INT32_MAX_SINGLE )
        {
            assertEquals( "Compressed bytesize for value " + value + " is too large", 2, delta );
        }
        else if ( value >= INT32_MIN_DOUBLE && value <= INT32_MAX_DOUBLE )
        {
            assertEquals( "Compressed bytesize for value " + value + " is too large", 3, delta );
        }
        else if ( value >= INT32_MIN_TRIPPLE && value <= INT32_MAX_TRIPPLE )
        {
            assertEquals( "Compressed bytesize for value " + value + " is too large", 4, delta );
        }
        else if ( value >= INT32_FULL && value <= INT32_FULL )
        {
            assertEquals( "Compressed bytesize for value " + value + " is too large", 5, delta );
        }
    }

    private void checkLongLength( long value, long delta )
    {
        if ( value >= INT64_MIN_SINGLE && value <= INT64_MAX_SINGLE )
        {
            assertEquals( "Compressed bytesize for value " + value + " is too large", 2, delta );
        }
        else if ( value >= INT64_MIN_DOUBLE && value <= INT64_MAX_DOUBLE )
        {
            assertEquals( "Compressed bytesize for value " + value + " is too large", 3, delta );
        }
        else if ( value >= INT64_MIN_TRIPPLE && value <= INT64_MAX_TRIPPLE )
        {
            assertEquals( "Compressed bytesize for value " + value + " is too large", 4, delta );
        }
        else if ( value >= INT64_MIN_QUAD && value <= INT64_MAX_QUAD )
        {
            assertEquals( "Compressed bytesize for value " + value + " is too large", 5, delta );
        }
        else if ( value >= INT64_MIN_FIFTH && value <= INT64_MAX_FIFTH )
        {
            assertEquals( "Compressed bytesize for value " + value + " is too large", 6, delta );
        }
        else if ( value >= INT64_MIN_SIXTH && value <= INT64_MAX_SIXTH )
        {
            assertEquals( "Compressed bytesize for value " + value + " is too large", 7, delta );
        }
        else if ( value >= INT64_MIN_SEVENTH && value <= INT64_MAX_SEVENTH )
        {
            assertEquals( "Compressed bytesize for value " + value + " is too large", 8, delta );
        }
        else if ( value >= INT64_FULL && value <= INT64_FULL )
        {
            assertEquals( "Compressed bytesize for value " + value + " is too large", 9, delta );
        }
    }

}
