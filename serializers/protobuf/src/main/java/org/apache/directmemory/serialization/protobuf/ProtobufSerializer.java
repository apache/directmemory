package org.apache.directmemory.serialization.protobuf;

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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Method;

import org.apache.directmemory.serialization.Serializer;
import org.kohsuke.MetaInfServices;

import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.Message;

public final class ProtobufSerializer
    implements Serializer
{

    private static final String NEW_BUILDER_METHOD = "newBuilder";

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> byte[] serialize( T obj )
        throws IOException
    {
        checkProtobufMessage( obj.getClass() );

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try
        {
            ( (Message) obj ).writeTo( baos );
        }
        finally
        {
            try
            {
                baos.close();
            }
            catch ( Exception e )
            {
                // close quietly
            }
        }

        return baos.toByteArray();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T deserialize( byte[] source, Class<T> clazz )
        throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException
    {
        clazz = checkProtobufMessage( clazz );

        try
        {
            Method newBuilder = clazz.getMethod( NEW_BUILDER_METHOD );

            // fixme no idea ATM how to fix type inference here
            GeneratedMessage.Builder builder = (GeneratedMessage.Builder) newBuilder.invoke( clazz );

            @SuppressWarnings( "unchecked" ) // cast should be safe since it is driven by the type
            T deserialized = (T) builder.mergeFrom( source ).build();

            return deserialized;
        }
        catch ( Throwable t )
        {
            throw new IOException( t );
        }
    }

    private static <T> Class<T> checkProtobufMessage( Class<T> clazz )
    {
        if ( !Message.class.isAssignableFrom( clazz ) )
        {
            throw new IllegalArgumentException( format( "Class %s cannot be serialized via Google Protobuf, it is not a %s",
                                                        clazz.getName(),
                                                        Message.class.getName() ) );
        }
        return clazz;
    }

}
