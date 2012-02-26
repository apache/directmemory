package org.apache.directmemory.server.services;
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

import org.apache.directmemory.serialization.SerializerFactory;
import org.apache.directmemory.serialization.StandardSerializer;
import org.apache.directmemory.server.commons.DirectMemoryRequest;
import org.apache.directmemory.server.commons.DirectMemoryResponse;
import org.apache.directmemory.server.commons.ExchangeType;
import org.apache.directmemory.test.Wine;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author Olivier Lamy
 */
public class ServletWithClientTextPlainTypeTest
    extends AbstractServletWithClientTest
{
    @Override
    protected ExchangeType getExchangeType()
    {
        return ExchangeType.TEXT_PLAIN;
    }

    @Test
    public void putAndGet()
        throws Exception
    {
        client.put( new DirectMemoryRequest<String>( "bordeaux", "very great wine" ).setSerializer(
            SerializerFactory.createNewSerializer( StandardSerializer.class ) ) );

        DirectMemoryRequest<String> rq = new DirectMemoryRequest<String>( "bordeaux", String.class );

        DirectMemoryResponse<String> response = client.retrieve( rq );

        assertTrue( response.isFound() );
        String wine = response.getResponse();

        assertEquals( "very great wine", wine );
    }

    @Test
    public void putAndGetAndDelete()
        throws Exception
    {

        client.put( new DirectMemoryRequest<String>( "bordeaux", "very great wine" ).setSerializer(
            SerializerFactory.createNewSerializer( StandardSerializer.class ) ) );

        DirectMemoryResponse<String> response =
            client.retrieve( new DirectMemoryRequest( "bordeaux", "very great wine" ) );

        assertTrue( response.isFound() );
        assertEquals( "very great wine", response.getResponse() );

        DirectMemoryResponse deleteResponse = client.delete( new DirectMemoryRequest<Wine>( "bordeaux" ) );
        assertTrue( deleteResponse.isDeleted() );

        response = client.retrieve( new DirectMemoryRequest<String>( "bordeaux", String.class ) );

        assertFalse( response.isFound() );
        String res = response.getResponse();
        assertNull( res );
    }

    @Test
    public void putSmallExpiresAndGetNotFound()
        throws Exception
    {

        client.delete( new DirectMemoryRequest<String>( "bordeaux" ) );

        client.put( new DirectMemoryRequest<String>( "bordeaux", "very great wine" ).setSerializer(
            SerializerFactory.createNewSerializer( StandardSerializer.class ) ).setExpiresIn( 1000 ) );

        DirectMemoryRequest<String> rq = new DirectMemoryRequest<String>( "bordeaux", "very great wine" ).setSerializer(
            SerializerFactory.createNewSerializer( StandardSerializer.class ) );

        DirectMemoryResponse<String> response = client.retrieve( rq );

        assertTrue( response.isFound() );
        String wine = response.getResponse();

        assertEquals( "very great wine", wine );

        Thread.sleep( 10001 );

        rq = new DirectMemoryRequest<String>( "bordeaux", "very great wine" );

        response = client.retrieve( rq );

        assertFalse( response.isFound() );

        assertNull( response.getResponse() );
    }
}
