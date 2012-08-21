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

import java.util.Random;

public class MemoryTestUtils
{

    private static final String PAYLOAD_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

    private static final Random R = new Random();

    public static byte[] generateRandomPayload( int sizeInByte )
    {
        final StringBuilder sb = new StringBuilder( sizeInByte );
        for ( int i = 0; i < sizeInByte; i++ )
        {
            sb.append( PAYLOAD_CHARS.charAt( R.nextInt( PAYLOAD_CHARS.length() ) ) );
        }
        return sb.toString().getBytes();
    }

}
