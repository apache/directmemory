/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.directmemory.memory.buffer;

import java.lang.reflect.Constructor;

import javax.xml.transform.Source;

/*
 * This codebase is derived from the org.apache.lucene.util.UnicodeUtil class from Apache Lucene project.
 */

/*
 * Some of this code came from the excellent Unicode
 * conversion examples from:
 *
 *   http://www.unicode.org/Public/PROGRAMS/CVTUTF
 *
 * Full Copyright for that code follows:
 */

/*
 * Copyright 2001-2004 Unicode, Inc.
 * 
 * Disclaimer
 * 
 * This source code is provided as is by Unicode, Inc. No claims are
 * made as to fitness for any particular purpose. No warranties of any
 * kind are expressed or implied. The recipient agrees to determine
 * applicability of information provided. If this file has been
 * purchased on magnetic or optical media from Unicode, Inc., the
 * sole remedy for any claim will be exchange of defective media
 * within 90 days of receipt.
 * 
 * Limitations on Rights to Redistribute This Code
 * 
 * Unicode, Inc. hereby grants the right to freely use the information
 * supplied in this file in the creation of products supporting the
 * Unicode Standard, and to make copies of this file in any form
 * for internal or external distribution as long as this notice
 * remains attached.
 */

/*
 * Additional code came from the IBM ICU library.
 *
 *  http://www.icu-project.org
 *
 * Full Copyright for that code follows.
 */

/*
 * Copyright (C) 1999-2010, International Business Machines
 * Corporation and others.  All Rights Reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, and/or sell copies of the
 * Software, and to permit persons to whom the Software is furnished to do so,
 * provided that the above copyright notice(s) and this permission notice appear
 * in all copies of the Software and that both the above copyright notice(s) and
 * this permission notice appear in supporting documentation.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT OF THIRD PARTY RIGHTS.
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR HOLDERS INCLUDED IN THIS NOTICE BE
 * LIABLE FOR ANY CLAIM, OR ANY SPECIAL INDIRECT OR CONSEQUENTIAL DAMAGES, OR
 * ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER
 * IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT
 * OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *
 * Except as contained in this notice, the name of a copyright holder shall not
 * be used in advertising or otherwise to promote the sale, use or other
 * dealings in this Software without prior written authorization of the
 * copyright holder.
 */

/**
 * Class to encode java's UTF16 char[] into UTF8 byte[] without always allocating a new byte[] as
 * String.getBytes("UTF-8") does.
 */
public final class UnicodeUtil
{

    private UnicodeUtil()
    {
    } // no instance

    public static final int UNI_SUR_HIGH_START = 0xD800;

    public static final int UNI_SUR_HIGH_END = 0xDBFF;

    public static final int UNI_SUR_LOW_START = 0xDC00;

    public static final int UNI_SUR_LOW_END = 0xDFFF;

    public static final int UNI_REPLACEMENT_CHAR = 0xFFFD;

    private static final long UNI_MAX_BMP = 0x0000FFFF;

    private static final long HALF_SHIFT = 10;

    private static final long HALF_MASK = 0x3FFL;

    private static final int SURROGATE_OFFSET = Character.MIN_SUPPLEMENTARY_CODE_POINT
        - ( UNI_SUR_HIGH_START << HALF_SHIFT ) - UNI_SUR_LOW_START;

    // Special String package private internal constructor for sharing char-array usage
    private static final Constructor<String> STRING_PP_CONSTRUCTOR;

    static
    {
        Constructor<String> constructor = null;
        try
        {
            constructor = String.class.getDeclaredConstructor( int.class, int.class, char[].class );
            constructor.setAccessible( true );
        }
        catch ( SecurityException e )
        {
            // intentionally left blank
        }
        catch ( NoSuchMethodException e )
        {
            // intentionally left blank
        }
        STRING_PP_CONSTRUCTOR = constructor;
    }

    /**
     * Encode characters from the given {@link String}, starting at offset 0 for length chars. Returns length of the
     * encoded String in bytes.
     */
    public static int UTF16toUTF8( String value, WritableMemoryBuffer target )
    {
        char[] characters = value.toCharArray();
        int length = characters.length;

        // Write string length to target
        target.writeInt( length );

        int i = 0;
        final int end = length;

        int writtenBytes = 0;
        while ( i < end )
        {

            final int code = characters[i++];

            if ( code < 0x80 )
            {
                target.writeByte( (byte) code );
                writtenBytes++;
            }
            else if ( code < 0x800 )
            {
                target.writeByte( (byte) ( 0xC0 | ( code >> 6 ) ) );
                target.writeByte( (byte) ( 0x80 | ( code & 0x3F ) ) );
                writtenBytes += 2;
            }
            else if ( code < 0xD800 || code > 0xDFFF )
            {
                target.writeByte( (byte) ( 0xE0 | ( code >> 12 ) ) );
                target.writeByte( (byte) ( 0x80 | ( ( code >> 6 ) & 0x3F ) ) );
                target.writeByte( (byte) ( 0x80 | ( code & 0x3F ) ) );
                writtenBytes += 3;
            }
            else
            {
                // surrogate pair
                // confirm valid high surrogate
                if ( code < 0xDC00 && i < end )
                {
                    int utf32 = characters[i];
                    // confirm valid low surrogate and write pair
                    if ( utf32 >= 0xDC00 && utf32 <= 0xDFFF )
                    {
                        utf32 = ( code << 10 ) + utf32 + SURROGATE_OFFSET;
                        i++;
                        target.writeByte( (byte) ( 0xF0 | ( utf32 >> 18 ) ) );
                        target.writeByte( (byte) ( 0x80 | ( ( utf32 >> 12 ) & 0x3F ) ) );
                        target.writeByte( (byte) ( 0x80 | ( ( utf32 >> 6 ) & 0x3F ) ) );
                        target.writeByte( (byte) ( 0x80 | ( utf32 & 0x3F ) ) );
                        writtenBytes += 4;
                        continue;
                    }
                }
                // replace unpaired surrogate or out-of-order low surrogate
                // with substitution character
                target.writeByte( (byte) 0xEF );
                target.writeByte( (byte) 0xBF );
                target.writeByte( (byte) 0xBD );
                writtenBytes += 3;
            }
        }
        return writtenBytes;
    }

    public static boolean validUTF16String( CharSequence s )
    {
        final int size = s.length();
        for ( int i = 0; i < size; i++ )
        {
            char ch = s.charAt( i );
            if ( ch >= UNI_SUR_HIGH_START && ch <= UNI_SUR_HIGH_END )
            {
                if ( i < size - 1 )
                {
                    i++;
                    char nextCH = s.charAt( i );
                    if ( nextCH >= UNI_SUR_LOW_START && nextCH <= UNI_SUR_LOW_END )
                    {
                        // Valid surrogate pair
                    }
                    else
                        // Unmatched high surrogate
                        return false;
                }
                else
                    // Unmatched high surrogate
                    return false;
            }
            else if ( ch >= UNI_SUR_LOW_START && ch <= UNI_SUR_LOW_END )
                // Unmatched low surrogate
                return false;
        }

        return true;
    }

    public static boolean validUTF16String( char[] s, int size )
    {
        for ( int i = 0; i < size; i++ )
        {
            char ch = s[i];
            if ( ch >= UNI_SUR_HIGH_START && ch <= UNI_SUR_HIGH_END )
            {
                if ( i < size - 1 )
                {
                    i++;
                    char nextCH = s[i];
                    if ( nextCH >= UNI_SUR_LOW_START && nextCH <= UNI_SUR_LOW_END )
                    {
                        // Valid surrogate pair
                    }
                    else
                        return false;
                }
                else
                    return false;
            }
            else if ( ch >= UNI_SUR_LOW_START && ch <= UNI_SUR_LOW_END )
                // Unmatched low surrogate
                return false;
        }

        return true;
    }

    /**
     * Interprets the bytes from the given {@link Source} as UTF-8 and converts to UTF-16.
     * <p>
     * NOTE: Full characters are read, even if this reads past the length passed (and can result in an
     * ArrayOutOfBoundsException if invalid UTF-8 is passed). Explicit checks for valid UTF-8 are not performed.
     */
    public static String UTF8toUTF16( ReadableMemoryBuffer source )
    {
        int charLength = source.readInt();

        int offset = 0;
        final char[] out = new char[charLength];
        while ( offset < charLength )
        {
            int b = source.readByte() & 0xff;
            if ( b < 0xc0 )
            {
                assert b < 0x80;
                out[offset++] = (char) b;
            }
            else if ( b < 0xe0 )
            {
                out[offset++] = (char) ( ( ( b & 0x1f ) << 6 ) + ( source.readByte() & 0x3f ) );
            }
            else if ( b < 0xf0 )
            {
                out[offset++] =
                    (char) ( ( ( b & 0xf ) << 12 ) + ( ( source.readByte() & 0x3f ) << 6 ) + ( source.readByte() & 0x3f ) );
            }
            else
            {
                assert b < 0xf8 : "b = 0x" + Integer.toHexString( b );
                int ch =
                    ( ( b & 0x7 ) << 18 ) + ( ( source.readByte() & 0x3f ) << 12 )
                        + ( ( source.readByte() & 0x3f ) << 6 ) + ( source.readByte() & 0x3f );
                if ( ch < UNI_MAX_BMP )
                {
                    out[offset++] = (char) ch;
                }
                else
                {
                    int chHalf = ch - 0x0010000;
                    out[offset++] = (char) ( ( chHalf >> 10 ) + 0xD800 );
                    out[offset++] = (char) ( ( chHalf & HALF_MASK ) + 0xDC00 );
                }
            }
        }

        if ( STRING_PP_CONSTRUCTOR != null )
        {
            return new String( out );
        }
        else
        {
            try
            {
                return STRING_PP_CONSTRUCTOR.newInstance( 0, out.length, out );
            }
            catch ( Exception e )
            {
                return new String( out );
            }
        }
    }

}
