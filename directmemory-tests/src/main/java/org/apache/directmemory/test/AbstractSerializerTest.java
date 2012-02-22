package org.apache.directmemory.test;

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

import org.apache.directmemory.serialization.Serializer;
import org.apache.directmemory.serialization.SerializerFactory;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;

/**
 * A kind of tck test for serializer.
 */
public abstract class AbstractSerializerTest
{
    public abstract String getSerializerClassName();

    @Test
    public void factoryWithFQDN()
        throws Exception
    {
        assertEquals( getSerializerClassName(),
                      SerializerFactory.createNewSerializer( getSerializerClassName() ).getClass().getName() );
    }

    @Test
    public void simpleSerialization()
        throws Exception
    {
        Wine wine = new Wine( "Gevrey-Chambertin", "nice French wine from Bourgogne" );
        Serializer serializer = SerializerFactory.createNewSerializer( getSerializerClassName() );

        byte[] bytes = serializer.serialize( wine );

        Wine newWine = serializer.deserialize( bytes, Wine.class );

        assertEquals( wine.getName(), newWine.getName() );
        assertEquals( wine.getDescription(), newWine.getDescription() );

    }
}
