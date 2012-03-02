package org.apache.directmemory.memory.allocator;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Utility class around direct {@link ByteBuffer} 
 *
 */
public class DirectByteBufferUtils
{

    /**
     * Clear and release the internal content of a direct {@link ByteBuffer}.
     * Clearing manually the content avoid waiting till the GC do his job.
     * @param buffer : the buffer to clear
     * @throws IllegalArgumentException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     * @throws SecurityException
     * @throws NoSuchMethodException
     */
    public static void destroyDirectByteBuffer( final ByteBuffer buffer )
        throws IllegalArgumentException, IllegalAccessException, InvocationTargetException, SecurityException,
        NoSuchMethodException
    {

        checkArgument( buffer.isDirect(), "toBeDestroyed isn't direct!" );

        Method cleanerMethod = buffer.getClass().getMethod( "cleaner" );
        cleanerMethod.setAccessible( true );
        Object cleaner = cleanerMethod.invoke( buffer );
        Method cleanMethod = cleaner.getClass().getMethod( "clean" );
        cleanMethod.setAccessible( true );
        cleanMethod.invoke( cleaner );

    }
    
}
