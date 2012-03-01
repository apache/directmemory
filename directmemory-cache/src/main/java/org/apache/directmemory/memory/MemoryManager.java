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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemoryManager
{
    private static Logger logger = LoggerFactory.getLogger( MemoryManager.class );

    private static MemoryManagerService<Object> memoryManager = new MemoryManagerServiceImpl<Object>();

    private MemoryManager()
    {
        //static class
    }

    public static void init( int numberOfBuffers, int size )
    {
        memoryManager.init( numberOfBuffers, size );
    }

    public static Pointer<Object> store( byte[] payload, int expiresIn )
    {
        return memoryManager.store( payload, expiresIn );
    }

    public static Pointer<Object> store( byte[] payload )
    {
        return store( payload, 0 );
    }

    public static Pointer<Object> update( Pointer<Object> pointer, byte[] payload )
    {
        return memoryManager.update( pointer, payload );
    }

    public static byte[] retrieve( Pointer<Object> pointer )
    {
        return memoryManager.retrieve( pointer );
    }

    public static void free( Pointer<Object> pointer )
    {
        memoryManager.free( pointer );
    }

    public static void clear()
    {
        memoryManager.clear();
    }

    public static long capacity()
    {
        return memoryManager.capacity();
    }

    public static long collectExpired()
    {
        return memoryManager.collectExpired();
    }

    public static void collectLFU()
    {
        memoryManager.collectLFU();
    }

    public static MemoryManagerService<Object> getMemoryManager()
    {
        return memoryManager;
    }

    public static Pointer<Object> allocate( int size )
    {
        return memoryManager.allocate( Object.class, size, -1, -1 ); //add a version with expiration
    }
}
