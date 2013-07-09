package org.apache.directmemory.serialization.kryo;

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

import org.apache.directmemory.serialization.Serializer;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.Closeable;
import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public final class KryoSerializer
    implements Serializer, Closeable
{
    /* buffer size */
    private static final int BUFFER_SIZE = 1024;

    private final KryoPool pool;

    public KryoSerializer(KryoPool pool)
    {
        this.pool = pool;
    }

    public KryoSerializer()
    {
        this(new KryoPool());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> byte[] serialize(T obj)
            throws IOException
    {
        Class<?> clazz = obj.getClass();

        KryoHolder kh = null;
        try
        {
            kh = pool.get();
            kh.reset();
            checkRegiterNeeded(kh.kryo, clazz);

            kh.kryo.writeObject(kh.output, obj);
            return kh.output.toBytes();
        }
        finally
        {
            if (kh != null)
            {
                pool.done(kh);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T deserialize( byte[] source, Class<T> clazz )
        throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException
    {
        KryoHolder kh = null;
        try
        {
            kh = pool.get();
            checkRegiterNeeded(kh.kryo, clazz);

            Input input = new Input(source);
            return kh.kryo.readObject(input, clazz);
        }
        finally
        {
            if (kh != null)
            {
                pool.done(kh);
            }
        }
    }

    /**
     * Closes the pool releasing any associated Kryo instance with it
     * @throws IOException
     */
    @Override
    public void close() throws IOException
    {
        pool.close();
    }

    private static void checkRegiterNeeded(Kryo kryo, Class<?> clazz)
    {
        kryo.register( clazz );
    }



    private static class KryoHolder
    {
        final Kryo kryo;
        final Output output = new Output(BUFFER_SIZE, -1);

        KryoHolder(Kryo kryo)
        {
            this.kryo = kryo;
        }

        private void reset()
        {
            output.clear();
        }
    }

    public static class KryoPool
    {
        private final Queue<KryoHolder> objects = new ConcurrentLinkedQueue<KryoHolder>();

        public KryoHolder get()
        {
            KryoHolder kh;
            if ((kh = objects.poll()) == null)
            {
                kh = new KryoHolder(createInstance());
            }
            return kh;
        }

        public void done(KryoHolder kh)
        {
            objects.offer(kh);
        }

        public void close()
        {
            objects.clear();
        }

        /**
         * Sub classes can customize the Kryo instance by overriding this method
         *
         * @return create Kryo instance
         */
        protected Kryo createInstance()
        {
            Kryo kryo = new Kryo();
            kryo.setReferences(false);
            return kryo;
        }

    }

}
