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
public class DirectMemoryCacheConstants
{

    public static final String XML_REQUEST_ROOT_ELEM_NAME = "DirectMemoryRQ";

    public static final String XML_RESPONSE_ROOT_ELEM_NAME = "DirectMemoryRS";

    public static final String CACHE_CONTENT_ELEM_NAME = "cacheContent";

    public static final String PUT_ATT_NAME = "put";

    public static final String KEY_ATT_NAME = "key";

    public static final String EXPIRES_IN_ATT_NAME = "expiresIn";

    public static final String UPDATED_ATT_NAME = "updated";

    public static final String FOUND_ATT_NAME = "found";

    public static final String SERIALIZER_FIELD_NAME = "serializer";

    private DirectMemoryCacheConstants()
    {
        // no op
    }
}
