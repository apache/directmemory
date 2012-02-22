package org.apache.directmemory.serialization.msgpack;

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

import org.apache.directmemory.serialization.Serializer;
import org.kohsuke.MetaInfServices;
import org.msgpack.MessagePack;
import org.msgpack.annotation.Message;

// olamy currently this doesn't work so use manual file
//@MetaInfServices
public final class MessagePackSerializer
    implements Serializer
{

    private final MessagePack msgpack = new MessagePack();

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> byte[] serialize( T obj )
        throws IOException
    {
        Class<?> clazz = obj.getClass();

        if ( !clazz.isAnnotationPresent( Message.class )
                        && msgpack.lookup( clazz ) == null )
        {
            msgpack.register( clazz );
        }

        return msgpack.write( obj );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T deserialize( byte[] source, Class<T> clazz )
        throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException
    {
        return msgpack.read( source, clazz );
    }

}
