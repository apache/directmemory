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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;

import org.apache.directmemory.measures.Monitor;
import org.apache.directmemory.measures.MonitorService;
import org.apache.directmemory.measures.Ram;
import org.junit.Test;

public final class ProtostuffTestCase
{

    private void testSerializer( String name, Serializer serializer, int size, int howMany )
        throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException
    {
        MonitorService stopWatch = Monitor.get( "serializer." + name + "." + size + "bytes" );
        MonitorService stopWatch2 = Monitor.get( "deserializer." + name + "." + size + "bytes" );
        DummyPojo pojo = new DummyPojo( "test", size );
        for ( int i = 0; i < howMany; i++ )
        {
            long split = stopWatch.start();
            final byte[] array = serializer.serialize( pojo );
            stopWatch.stop( split );
            long split2 = stopWatch2.start();
            DummyPojo check = serializer.deserialize( array, pojo.getClass() );
            stopWatch2.stop( split2 );
            assertNotNull( "object has not been serialized", check );
            assertEquals( pojo.name, check.name );
        }
    }

    @Test
    public void ProtostuffTest()
        throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException
    {
        testSerializer( "protostuff-old", new ProtoStuffSerializerV1(), Ram.Kb( 1 ), 20000 );
        testSerializer( "protostuff-old", new ProtoStuffSerializerV1(), Ram.Kb( 2 ), 20000 );
        testSerializer( "protostuff-old", new ProtoStuffSerializerV1(), Ram.Kb( 3 ), 20000 );
        testSerializer( "protostuff-old", new ProtoStuffSerializerV1(), Ram.Kb( 4 ), 20000 );
    }

    @Test
    public void ProtostuffV2Test()
        throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException
    {
        testSerializer( "protostuff-new", new ProtoStuffWithLinkedBufferSerializer(), Ram.Kb( 1 ), 20000 );
        testSerializer( "protostuff-new", new ProtoStuffWithLinkedBufferSerializer(), Ram.Kb( 2 ), 20000 );
        testSerializer( "protostuff-new", new ProtoStuffWithLinkedBufferSerializer(), Ram.Kb( 3 ), 20000 );
        testSerializer( "protostuff-new", new ProtoStuffWithLinkedBufferSerializer(), Ram.Kb( 4 ), 20000 );
        testSerializer( "cinquantamila", new ProtoStuffWithLinkedBufferSerializer(), Ram.Kb( 3 ), 50000 );
    }

}
