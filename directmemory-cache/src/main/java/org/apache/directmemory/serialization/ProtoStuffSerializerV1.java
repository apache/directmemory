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

import java.io.IOException;

import org.apache.directmemory.measures.Ram;

import com.dyuproject.protostuff.LinkedBuffer;
import com.dyuproject.protostuff.ProtostuffIOUtil;
import com.dyuproject.protostuff.Schema;
import com.dyuproject.protostuff.runtime.RuntimeSchema;

public class ProtoStuffSerializerV1
    implements Serializer
{

    static int serBufferSize = Ram.Kb( 3 );
//	static int serBufferSize = 300;

    /* (non-Javadoc)
      * @see org.apache.directmemory.utils.Serializer#serialize(java.lang.Object, java.lang.Class)
      */
    @SuppressWarnings( "unchecked" )
    public byte[] serialize( Object obj, @SuppressWarnings( "rawtypes" ) Class clazz )
        throws IOException
    {
        @SuppressWarnings( "rawtypes" ) Schema schema = RuntimeSchema.getSchema( clazz );
        final LinkedBuffer buffer = LinkedBuffer.allocate( serBufferSize );
        byte[] protostuff = null;

        try
        {
            protostuff = ProtostuffIOUtil.toByteArray( obj, schema, buffer );
        }
        finally
        {
            buffer.clear();
        }
        return protostuff;
    }

    /* (non-Javadoc)
      * @see org.apache.directmemory.utils.Serializer#deserialize(byte[], java.lang.Class)
      */
    @SuppressWarnings( "unchecked" )
    public Object deserialize( byte[] source, @SuppressWarnings( "rawtypes" ) Class clazz )
        throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException
    {
        final Object object = clazz.newInstance();
        @SuppressWarnings( "rawtypes" )
        final Schema schema = RuntimeSchema.getSchema( clazz );
        ProtostuffIOUtil.mergeFrom( source, object, schema );
        return object;
    }
}
