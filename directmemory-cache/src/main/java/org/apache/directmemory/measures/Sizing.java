package org.apache.directmemory.measures;

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

import static java.lang.String.format;

public class Sizing
{

    private static final int KILOBYTE_UNIT = 1024;

    public static int Gb( double giga )
    {
        return (int) giga * KILOBYTE_UNIT * KILOBYTE_UNIT * KILOBYTE_UNIT;
    }

    public static int Mb( double mega )
    {
        return (int) mega * KILOBYTE_UNIT * KILOBYTE_UNIT;
    }

    public static int Kb( double kilo )
    {
        return (int) kilo * KILOBYTE_UNIT;
    }

    public static int unlimited()
    {
        return -1;
    }

    public static String inKb( long bytes )
    {
        return format( "%(,.1fKb", (double) bytes / KILOBYTE_UNIT );
    }

    public static String inMb( long bytes )
    {
        return format( "%(,.1fMb", (double) bytes / KILOBYTE_UNIT / KILOBYTE_UNIT );
    }

    public static String inGb( long bytes )
    {
        return format( "%(,.1fKb", (double) bytes / KILOBYTE_UNIT / KILOBYTE_UNIT / KILOBYTE_UNIT );
    }

}
