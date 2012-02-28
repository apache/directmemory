package org.apache.directmemory.server.commons;
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/**
 * @author Olivier Lamy
 */
public class DirectMemoryHttpConstants
{
    public static final String JAVA_SERIALIZED_OBJECT_CONTENT_TYPE_HEADER = "application/x-java-serialized-object";

    public static final String SERIALIZER_HTTP_HEADER = "X-DirectMemory-Serializer";

    public static final String EXPIRES_IN_HTTP_HEADER = "X-DirectMemory-ExpiresIn";

    public static final String EXPIRES_SERIALIZE_SIZE = "X-DirectMemory-SerializeSize";

    private DirectMemoryHttpConstants()
    {
        // no op only a constants class
    }
}
