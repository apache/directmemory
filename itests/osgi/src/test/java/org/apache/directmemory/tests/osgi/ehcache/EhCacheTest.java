package org.apache.directmemory.tests.osgi.ehcache;

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
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import net.sf.ehcache.CacheException;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Ehcache;
import net.sf.ehcache.Element;
import org.apache.directmemory.tests.osgi.DirectMemoryOsgiTestSupport;
import org.apache.directmemory.tests.osgi.cache.SimpleObject;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Customizer;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.Configuration;
import org.ops4j.pax.exam.junit.JUnit4TestRunner;
import org.ops4j.pax.exam.options.MavenArtifactProvisionOption;
import org.osgi.framework.Constants;

import static org.ops4j.pax.exam.CoreOptions.equinox;
import static org.ops4j.pax.exam.CoreOptions.felix;
import static org.ops4j.pax.exam.CoreOptions.mavenBundle;
import static org.ops4j.pax.exam.OptionUtils.combine;
import static org.ops4j.pax.swissbox.tinybundles.core.TinyBundles.modifyBundle;

@RunWith(JUnit4TestRunner.class)
public class EhCacheTest extends DirectMemoryOsgiTestSupport {

  @Test
  public void testPutRetreive() {
    Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
    CacheManager cacheManager = CacheManager.getInstance();
    Ehcache ehcache = cacheManager.getEhcache("testCache");

    ehcache.put(new Element("testKey", "testValue"));
    stats(ehcache);
    Assert.assertEquals("testValue", ehcache.get("testKey").getObjectValue());
  }

  @Test
  public void testSizing() {
    Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
    CacheManager cacheManager = CacheManager.getInstance();
    Ehcache ehcache = cacheManager.getEhcache("testCache");
    for (int i = 0; i < 30000; i++) {
      if ((i % 1000) == 0) {
        System.out.println("heatbeat " + i);
        stats(ehcache);
      }
      ehcache.put(new Element(i, new byte[1024]));
    }
    stats(ehcache);
    Assert.assertTrue(true);
  }

  @Test
  public void testOffHeapExceedMemory()
          throws IOException {
    Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
    CacheManager cacheManager = CacheManager.getInstance();
    Ehcache ehcache = cacheManager.getEhcache("testCache");
    Element element = null;
    try {
      for (int i = 0; i < 3000000; i++) {
        if ((i % 1000) == 0) {
          System.out.println("heatbeat 2 " + i);
          stats(ehcache);
        }
        element = new Element(i, new byte[1024]);
        ehcache.put(element);
      }
      Assert.fail("CacheException expected for DirectMemory OffHeap Memory Exceeded");
    } catch (CacheException e) {
      stats(ehcache);
      Assert.assertTrue("CacheException expected for DirectMemory OffHeap Memory Exceeded", true);
    }

  }

  private void stats(Ehcache ehcache) {
    System.out.println("OnHeapSize=" + ehcache.calculateInMemorySize() + ", OnHeapElements="
            + ehcache.getMemoryStoreSize());
    System.out.println("OffHeapSize=" + ehcache.calculateOffHeapSize() + ", OffHeapElements="
            + ehcache.getOffHeapStoreSize());
    System.out.println("DiskStoreSize=" + ehcache.calculateOnDiskSize() + ", DiskStoreElements="
            + ehcache.getDiskStoreSize());
  }

  public static Option[] getDynamicMemoryEhCacheOptions() {
    List<MavenArtifactProvisionOption> mavenOptions = Arrays.asList(
            mavenBundle().groupId("org.apache.servicemix.bundles").artifactId("org.apache.servicemix.bundles.ehcache").version(
                    System.getProperty( "ehcache.bundle.version", "2.6.6_1" )),
            mavenBundle().groupId("org.apache.directmemory").artifactId("directmemory-ehcache").version(
                    System.getProperty("direct.memory.version")));

    List<Option> options = new ArrayList<Option>();
    options.addAll(Arrays.asList(DirectMemoryOsgiTestSupport.getDynamicMemoryOptions()));
    options.addAll(mavenOptions);

    if (Boolean.getBoolean("osgi.debug")) {
      options.add(enabledDebuggingOnPort(Integer.getInteger("osgi.debug.port"),
              Boolean.getBoolean("osgi.debug.suspend")));
    }

    return options.toArray(new Option[options.size()]);
  }

  @Configuration
  public Option[] configure() {
    return combine(getDynamicMemoryEhCacheOptions(), new Customizer() {
      @Override
      public InputStream customizeTestProbe(InputStream testProbe) {
        return modifyBundle(testProbe)
                .add(SimpleObject.class)
                .add("/ehcache.xml", EhCacheTest.class.getResource("/ehcache.xml"))
                .set(Constants.DYNAMICIMPORT_PACKAGE, "*").build();
      }
    },
            //Uncomment the line below to debug test
            //enabledDebuggingOnPort(5005,true),
            felix(), equinox());
  }
}
