package org.apache.directmemory.serialization;

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

import static com.dyuproject.protostuff.LinkedBuffer.allocate;
import static com.dyuproject.protostuff.ProtostuffIOUtil.mergeFrom;
import static com.dyuproject.protostuff.ProtostuffIOUtil.toByteArray;
import static com.dyuproject.protostuff.runtime.RuntimeSchema.getSchema;

import java.io.IOException;

import org.apache.directmemory.measures.Ram;

import com.dyuproject.protostuff.LinkedBuffer;
import com.dyuproject.protostuff.Schema;

public final class ProtoStuffWithLinkedBufferSerializer
    implements Serializer
{

    static int bufferSize = Ram.Kb( 3 );

    /*
         *
         *
         *
        LinkedBuffer buffer8k = ...;
        try
        {
            ProtostuffIOUtil.writeTo(new ByteBufferOutputStream() { // paging logic }, message, schema, buffer8k)
        }
        finally
        {
            buffer8k.clear();
        }

        ProtostuffIOUtil.mergeFrom(new ByteArrayInputStream() { // paging logic}, message, schema, buffer8k);

    */

    private static final ThreadLocal<LinkedBuffer> localBuffer = new ThreadLocal<LinkedBuffer>()
    {
        protected LinkedBuffer initialValue()
        {
            return allocate( bufferSize );
        }
    };

    public ProtoStuffWithLinkedBufferSerializer()
    {

    }


    public ProtoStuffWithLinkedBufferSerializer( int bufferSize )
    {
        ProtoStuffWithLinkedBufferSerializer.bufferSize = bufferSize;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public <T> byte[] serialize( T obj )
        throws IOException
    {
        @SuppressWarnings( "unchecked" ) // type should be safe since got directly from the obj
        final Class<T> clazz = (Class<T>) obj.getClass();
        final Schema<T> schema = getSchema( clazz );

        final LinkedBuffer buffer = localBuffer.get();
        byte[] protostuff = null;

        try
        {
            protostuff = toByteArray( obj, schema, buffer );
        }
        finally
        {
            buffer.clear();
        }
        return protostuff;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T deserialize( byte[] source, Class<T> clazz )
        throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException
    {
        T object = clazz.newInstance();
        Schema<T> schema = getSchema( clazz );
        mergeFrom( source, object, schema );
        return object;
    }

}
