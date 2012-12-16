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

public class ByteOrderUtils
{

    static void putShort( short value, MemoryBuffer memoryBuffer, boolean bigEndian )
    {
        if ( bigEndian )
        {
            memoryBuffer.writeByte( (byte) ( value >> 8 ) );
            memoryBuffer.writeByte( (byte) ( value >> 0 ) );
        }
        else
        {
            memoryBuffer.writeByte( (byte) ( value >> 0 ) );
            memoryBuffer.writeByte( (byte) ( value >> 8 ) );
        }
    }

    static short getShort( MemoryBuffer memoryBuffer, boolean bigEndian )
    {
        if ( bigEndian )
        {
            byte b1 = memoryBuffer.readByte();
            byte b0 = memoryBuffer.readByte();
            return buildShort( b1, b0 );
        }
        else
        {
            byte b0 = memoryBuffer.readByte();
            byte b1 = memoryBuffer.readByte();
            return buildShort( b1, b0 );
        }
    }

    static void putInt( int value, MemoryBuffer memoryBuffer, boolean bigEndian )
    {
        if ( bigEndian )
        {
            memoryBuffer.writeByte( (byte) ( value >>> 24 ) );
            memoryBuffer.writeByte( (byte) ( value >>> 16 ) );
            memoryBuffer.writeByte( (byte) ( value >>> 8 ) );
            memoryBuffer.writeByte( (byte) ( value >>> 0 ) );
        }
        else
        {
            memoryBuffer.writeByte( (byte) ( value >>> 0 ) );
            memoryBuffer.writeByte( (byte) ( value >>> 8 ) );
            memoryBuffer.writeByte( (byte) ( value >>> 16 ) );
            memoryBuffer.writeByte( (byte) ( value >>> 24 ) );
        }
    }

    static int getInt( MemoryBuffer memoryBuffer, boolean bigEndian )
    {
        if ( bigEndian )
        {
            byte b3 = memoryBuffer.readByte();
            byte b2 = memoryBuffer.readByte();
            byte b1 = memoryBuffer.readByte();
            byte b0 = memoryBuffer.readByte();
            return buildInt( b3, b2, b1, b0 );
        }
        else
        {
            byte b0 = memoryBuffer.readByte();
            byte b1 = memoryBuffer.readByte();
            byte b2 = memoryBuffer.readByte();
            byte b3 = memoryBuffer.readByte();
            return buildInt( b3, b2, b1, b0 );
        }
    }

    static void putLong( long value, MemoryBuffer memoryBuffer, boolean bigEndian )
    {
        if ( bigEndian )
        {
            memoryBuffer.writeByte( (byte) ( value >> 56 ) );
            memoryBuffer.writeByte( (byte) ( value >> 48 ) );
            memoryBuffer.writeByte( (byte) ( value >> 40 ) );
            memoryBuffer.writeByte( (byte) ( value >> 32 ) );
            memoryBuffer.writeByte( (byte) ( value >> 24 ) );
            memoryBuffer.writeByte( (byte) ( value >> 16 ) );
            memoryBuffer.writeByte( (byte) ( value >> 8 ) );
            memoryBuffer.writeByte( (byte) ( value >> 0 ) );
        }
        else
        {
            memoryBuffer.writeByte( (byte) ( value >> 0 ) );
            memoryBuffer.writeByte( (byte) ( value >> 8 ) );
            memoryBuffer.writeByte( (byte) ( value >> 16 ) );
            memoryBuffer.writeByte( (byte) ( value >> 24 ) );
            memoryBuffer.writeByte( (byte) ( value >> 32 ) );
            memoryBuffer.writeByte( (byte) ( value >> 40 ) );
            memoryBuffer.writeByte( (byte) ( value >> 48 ) );
            memoryBuffer.writeByte( (byte) ( value >> 56 ) );
        }
    }

    static long getLong( MemoryBuffer memoryBuffer, boolean bigEndian )
    {
        if ( bigEndian )
        {
            byte b7 = memoryBuffer.readByte();
            byte b6 = memoryBuffer.readByte();
            byte b5 = memoryBuffer.readByte();
            byte b4 = memoryBuffer.readByte();
            byte b3 = memoryBuffer.readByte();
            byte b2 = memoryBuffer.readByte();
            byte b1 = memoryBuffer.readByte();
            byte b0 = memoryBuffer.readByte();
            return buildLong( b7, b6, b5, b4, b3, b2, b1, b0 );
        }
        else
        {
            byte b0 = memoryBuffer.readByte();
            byte b1 = memoryBuffer.readByte();
            byte b2 = memoryBuffer.readByte();
            byte b3 = memoryBuffer.readByte();
            byte b4 = memoryBuffer.readByte();
            byte b5 = memoryBuffer.readByte();
            byte b6 = memoryBuffer.readByte();
            byte b7 = memoryBuffer.readByte();
            return buildLong( b7, b6, b5, b4, b3, b2, b1, b0 );
        }
    }

    private static short buildShort( byte b1, byte b0 )
    {
        return (short) ( ( ( ( b1 & 0xFF ) << 8 ) | ( ( b0 & 0xFF ) << 0 ) ) );
    }

    private static int buildInt( byte b3, byte b2, byte b1, byte b0 )
    {
        return ( ( ( ( b3 & 0xFF ) << 24 ) | ( ( b2 & 0xFF ) << 16 ) | ( ( b1 & 0xFF ) << 8 ) | ( ( b0 & 0xFF ) << 0 ) ) );
    }

    private static long buildLong( byte b7, byte b6, byte b5, byte b4, byte b3, byte b2, byte b1, byte b0 )
    {
        return ( ( ( ( b7 & 0xFFL ) << 56 ) | ( ( b6 & 0xFFL ) << 48 ) | ( ( b5 & 0xFFL ) << 40 )
            | ( ( b4 & 0xFFL ) << 32 ) | ( ( b3 & 0xFFL ) << 24 ) | ( ( b2 & 0xFFL ) << 16 ) | ( ( b1 & 0xFFL ) << 8 ) | ( ( b0 & 0xFFL ) << 0 ) ) );
    }

}
