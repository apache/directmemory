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

package org.apache.directmemory.tests.osgi.cache;


import java.io.InputStream;
import org.apache.directmemory.cache.Cache;
import org.apache.directmemory.measures.Every;
import org.apache.directmemory.measures.Monitor;
import org.apache.directmemory.measures.Ram;
import org.apache.directmemory.memory.Pointer;
import org.apache.directmemory.tests.osgi.DirectMemoryOsgiTestSupport;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.ops4j.pax.exam.Customizer;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.Configuration;
import org.ops4j.pax.exam.junit.JUnit4TestRunner;
import org.osgi.framework.Constants;


import static org.junit.Assert.*;
import static org.ops4j.pax.exam.CoreOptions.equinox;
import static org.ops4j.pax.exam.CoreOptions.felix;
import static org.ops4j.pax.exam.CoreOptions.mavenBundle;
import static org.ops4j.pax.exam.OptionUtils.combine;
import static org.ops4j.pax.swissbox.tinybundles.core.TinyBundles.modifyBundle;

@RunWith(JUnit4TestRunner.class)
public class CacheTest extends DirectMemoryOsgiTestSupport{

  /**
   * This tests basic cache operations(put,retrieve) inside OSGi
   */
  @Test
  public void testCacheSingleton() {
    String key = "1";
    String obj = "Simple String Object";
    Cache.init(1, Ram.Mb(16));
		Cache.scheduleDisposalEvery(Every.seconds(1));
		Cache.dump();

    Pointer p = Cache.put("1", obj);
    Object result = Cache.retrieve("1");

    Cache.dump();
    Monitor.dump("cache");

    assertEquals(obj, result);
  }

  /**
   * This test basic cache operations(put,retrieve) inside OSGi using an object of an imported class (provided by an other bundle).
   */
  @Test
  public void testCacheSingletonWithImportedObject() {
    SimpleObject obj1 = new SimpleObject("1","Object One");
    SimpleObject obj2 = new SimpleObject("2","Object Two");
    Cache.init(1, Ram.Mb(16));
		Cache.scheduleDisposalEvery(Every.seconds(1));
		Cache.dump();

    Pointer p1 = Cache.put("1",obj1 );
    Pointer p2 = Cache.put("2",obj2 );
    Object result1 = Cache.retrieve("1");
    Object result2 = Cache.retrieve("2");

    Cache.dump();
    Monitor.dump("cache");

    assertEquals(obj1, result1);
    assertEquals(obj2, result2);
  }


  @Configuration
  public Option[] configure() {
    return combine(
            getDynamicMemoryOptions(),
            new Customizer() {
                    @Override
                    public InputStream customizeTestProbe(InputStream testProbe) {
                        return modifyBundle(testProbe)
                                .add(SimpleObject.class)
                                .set(Constants.DYNAMICIMPORT_PACKAGE, "*")
                                .build();
                    }
                },
            //Uncomment the line below to debug test
            //enabledDebuggingOnPort(5005,false),
            felix(),
            equinox()
    );
  }
}