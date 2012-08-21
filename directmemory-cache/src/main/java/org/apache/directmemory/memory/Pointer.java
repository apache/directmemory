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

import java.nio.ByteBuffer;

public interface Pointer<T>
{

    byte[] content();

    boolean isFree();

    void setFree( boolean free );

    boolean isExpired();

    float getFrequency();

    int getCapacity();

    void reset();

    int getBufferNumber();

    void setBufferNumber( int bufferNumber );

    int getStart();

    void setStart( int start );

    int getEnd();

    void setEnd( int end );

    void hit();

    Class<? extends T> getClazz();

    void setClazz( Class<? extends T> clazz );

    ByteBuffer getDirectBuffer();

    void setDirectBuffer( ByteBuffer directBuffer );

    void createdNow();

    void setExpiration( long expires, long expiresIn );

}
