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


import org.apache.directmemory.memory.OffHeapMemoryBuffer;
import org.apache.directmemory.memory.OffHeapMemoryBufferImpl;
import org.junit.Test;

public class OffHeapMemoryBufferTest extends AbstractOffHeapMemoryBufferTest
{

    protected OffHeapMemoryBuffer instanciateOffHeapMemoryBuffer( int bufferSize ) 
    {
        return OffHeapMemoryBufferImpl.createNew( bufferSize );
    }

    @Test
    public void testFullFillAndFreeAndClearBuffer()
    {
        // DIRECTMEMORY-40 : Pointers merging with adjacent free pointers when freeing.
    }
    
    @Test
    public void testStoreAllocAndFree()
    {
        // DIRECTMEMORY-40 : Pointers merging with adjacent free pointers when freeing.
    }
    
    @Test
    public void testUpdate()
    {
        // DIRECTMEMORY-49 : OffHeapMemoryBufferImpl.update does not reuse the same pointer
    }
}
