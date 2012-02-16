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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import org.apache.directmemory.measures.Ram;
import org.apache.directmemory.misc.DummyPojo;

public final class DummyPojoSerializer
    implements Serializer
{

    final DummyPojo pojo = new DummyPojo( "test", Ram.Kb( 2 ) );

    final byte[] data;

    public DummyPojoSerializer()
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try
        {
            ObjectOutputStream oos = new ObjectOutputStream( baos );
            oos.writeObject( pojo );
            oos.flush();
            oos.close();
        }
        catch ( Exception e )
        {
            // should not happen
        }
        data = baos.toByteArray();
    }

    @Override
    public <T> T deserialize( byte[] source, Class<T> clazz )
        throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException
    {
        // testing puts only
        return (T) pojo;
    }

    @Override
    public <T> byte[] serialize( T obj )
        throws IOException
    {
        return data;
    }

}
