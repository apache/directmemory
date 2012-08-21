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


import org.apache.directmemory.cache.CacheService;
import org.apache.directmemory.measures.Every;
import org.apache.directmemory.measures.Monitor;
import org.apache.directmemory.memory.Pointer;
import org.apache.directmemory.tests.osgi.DirectMemoryOsgiTestSupport;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Customizer;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.Configuration;
import org.ops4j.pax.exam.junit.JUnit4TestRunner;
import org.osgi.framework.Constants;

import java.io.IOException;
import java.io.InputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.ops4j.pax.exam.CoreOptions.equinox;
import static org.ops4j.pax.exam.CoreOptions.felix;
import static org.ops4j.pax.exam.OptionUtils.combine;
import static org.ops4j.pax.swissbox.tinybundles.core.TinyBundles.modifyBundle;
import static org.ops4j.pax.swissbox.tinybundles.core.TinyBundles.newBundle;

@RunWith( JUnit4TestRunner.class )
public class CacheServiceTest
    extends DirectMemoryOsgiTestSupport
{

    /**
     * This tests basic cache operations(put,retrieve) inside OSGi
     */
    @Test
    public void testCacheService()
    {
        String key = "2";
        String obj = "Simple String Object";
        CacheService cacheService = getOsgiService( CacheService.class );

        //Test retrieving an object added by an other bundle.
        Object result = cacheService.retrieve( "1" );
        assertNotNull( result );

        cacheService.scheduleDisposalEvery( Every.seconds( 1 ) );
        cacheService.dump();
        Monitor.dump( "cache" );

        Pointer p = cacheService.put( "2", obj );
        result = cacheService.retrieve( "2" );
        cacheService.dump();
        assertEquals( obj, result );
    }

    /**
     * This test basic cache operations(put,retrieve) inside OSGi using an object of an imported class (provided by an other bundle).
     */
    @Test
    public void testCacheServiceWithImportedObject()
    {
        SimpleObject obj1 = new SimpleObject( "2", "Object Two" );
        SimpleObject obj2 = new SimpleObject( "3", "Object Three" );

        CacheService cacheService = getOsgiService( CacheService.class );
        cacheService.scheduleDisposalEvery( Every.seconds( 1 ) );
        cacheService.dump();

        Pointer p1 = cacheService.put( "2", obj1 );
        Pointer p2 = cacheService.put( "3", obj2 );
        Object result1 = cacheService.retrieve( "2" );
        Object result2 = cacheService.retrieve( "3" );

        cacheService.dump();
        Monitor.dump( "cache" );

        assertEquals( obj1, result1 );
        assertEquals( obj2, result2 );
    }


    @Configuration
    public Option[] configure()
        throws IOException
    {
        return combine( getDynamicMemoryOptions(),

                        bundle( newBundle().add( SimpleObject.class ).add( CacheServiceExportingActivator.class ).set(
                            Constants.BUNDLE_ACTIVATOR, CacheServiceExportingActivator.class.getCanonicalName() ).set(
                            Constants.BUNDLE_SYMBOLICNAME,
                            "org.apache.directmemory.tests.osgi.cacheservice.exporter" ).set( Constants.BUNDLE_VERSION,
                                                                                              "1.0.0" ).set(
                            Constants.DYNAMICIMPORT_PACKAGE, "*" ).build() ).start(),

                        new Customizer()
                        {
                            @Override
                            public InputStream customizeTestProbe( InputStream testProbe )
                            {
                                return modifyBundle( testProbe ).set( Constants.DYNAMICIMPORT_PACKAGE, "*" ).build();
                            }
                        },
                        //Uncomment the line below to debug test
                        //enabledDebuggingOnPort(5005,true),
                        felix(), equinox() );
    }
}