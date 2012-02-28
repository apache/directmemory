package org.apache.directmemory.server.client;
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

import org.apache.directmemory.server.commons.DirectMemoryException;
import org.apache.directmemory.server.commons.DirectMemoryRequest;
import org.apache.directmemory.server.commons.DirectMemoryResponse;

import java.io.IOException;
import java.util.concurrent.Future;

/**
 * @author Olivier Lamy
 */
public interface DirectMemoryClient
{
    /**
     * <p>will ask the server if any content corresponding to the key passed in  {@link DirectMemoryRequest}</p>
     * <p>if the server doesn't return content {@link DirectMemoryResponse#isFound()} will be <code>false</code> </p>
     * @param directMemoryRequest
     * @return
     * @throws DirectMemoryException
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    DirectMemoryResponse retrieve( DirectMemoryRequest directMemoryRequest )
        throws DirectMemoryException, IOException, ClassNotFoundException, InstantiationException,
        IllegalAccessException;

    /**
     * same as retrieve
     */
    Future<DirectMemoryResponse> asyncRetrieve( DirectMemoryRequest directMemoryRequest )
        throws DirectMemoryException;


    DirectMemoryResponse put( DirectMemoryRequest directMemoryRequest )
        throws DirectMemoryException;

    /**
     *
     * same as put.
     */
    Future<DirectMemoryResponse> asyncPut( DirectMemoryRequest directMemoryRequest )
        throws DirectMemoryException;

    /**
     *
     * @param directMemoryRequest
     * @return check {@link DirectMemoryResponse#isDeleted()} to verify if the content has been deleted
     * @throws DirectMemoryException
     */
    DirectMemoryResponse delete( DirectMemoryRequest directMemoryRequest )
        throws DirectMemoryException;

    /**
     * same as async.
     * @param directMemoryRequest
     * @return
     * @throws DirectMemoryException
     */
    Future<DirectMemoryResponse> asyncDelete( DirectMemoryRequest directMemoryRequest )
        throws DirectMemoryException;
}
