package org.apache.directmemory.misc;

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

public class Format
{

    public static String it( String string, Object... args )
    {
        java.util.Formatter formatter = new java.util.Formatter();
        return formatter.format( string, args ).toString();
    }

    public static String logo()
    {
        return "         ____  _                 __  __  ___\r\n"
            + "        / __ \\(_)________  _____/ /_/  |/  /___  ____ ___  ____  _______  __\r\n"
            + "       / / / / // ___/ _ \\/ ___/ __/ /|_/ // _ \\/ __ `__ \\/ __ \\/ ___/ / / /\r\n"
            + "      / /_/ / // /  /  __/ /__/ /_/ /  / //  __/ / / / / / /_/ / /  / /_/ / \r\n"
            + "     /_____/_//_/   \\___/\\___/\\__/_/  /_/ \\___/_/ /_/ /_/\\____/_/   \\__, /\r\n"
            + "                                                                   /____/   ";
    }

}
