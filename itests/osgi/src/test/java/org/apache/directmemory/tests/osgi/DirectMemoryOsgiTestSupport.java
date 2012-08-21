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

package org.apache.directmemory.tests.osgi;

import org.ops4j.pax.exam.Inject;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.options.MavenArtifactProvisionOption;
import org.ops4j.pax.exam.options.UrlProvisionOption;
import org.ops4j.store.Store;
import org.ops4j.store.StoreFactory;
import org.osgi.framework.BundleContext;
import org.osgi.framework.Constants;
import org.osgi.framework.Filter;
import org.osgi.framework.FrameworkUtil;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.ServiceReference;
import org.osgi.util.tracker.ServiceTracker;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Dictionary;
import java.util.Enumeration;
import java.util.List;

import static org.ops4j.pax.exam.CoreOptions.mavenBundle;
import static org.ops4j.pax.exam.container.def.PaxRunnerOptions.vmOption;

public class DirectMemoryOsgiTestSupport
{

    public static final String JVM_DEBUG_OPTIONS = "-Xrunjdwp:transport=dt_socket,server=y,suspend=%s,address=%s";

    public static final String DO_SUSPEND = "y";

    public static final String DONT_SUSPEND = "n";

    public static final long DEFAULT_TIMEOUT = 30000;


    @Inject
    protected BundleContext bundleContext;

    protected <T> T getOsgiService( Class<T> type, long timeout )
    {
        return getOsgiService( type, null, timeout );
    }

    protected <T> T getOsgiService( Class<T> type )
    {
        return getOsgiService( type, null, DEFAULT_TIMEOUT );
    }

    /*
    * Explode the dictionary into a ,-delimited list of key=value pairs
    */
    private static String explode( Dictionary dictionary )
    {
        Enumeration keys = dictionary.keys();
        StringBuffer result = new StringBuffer();
        while ( keys.hasMoreElements() )
        {
            Object key = keys.nextElement();
            result.append( String.format( "%s=%s", key, dictionary.get( key ) ) );
            if ( keys.hasMoreElements() )
            {
                result.append( ", " );
            }
        }
        return result.toString();
    }

    protected <T> T getOsgiService( Class<T> type, String filter, long timeout )
    {
        ServiceTracker tracker = null;
        try
        {
            String flt;
            if ( filter != null )
            {
                if ( filter.startsWith( "(" ) )
                {
                    flt = "(&(" + Constants.OBJECTCLASS + "=" + type.getName() + ")" + filter + ")";
                }
                else
                {
                    flt = "(&(" + Constants.OBJECTCLASS + "=" + type.getName() + ")(" + filter + "))";
                }
            }
            else
            {
                flt = "(" + Constants.OBJECTCLASS + "=" + type.getName() + ")";
            }
            Filter osgiFilter = FrameworkUtil.createFilter( flt );
            tracker = new ServiceTracker( bundleContext, osgiFilter, null );
            tracker.open( true );

            Object svc = type.cast( tracker.waitForService( timeout ) );
            if ( svc == null )
            {
                Dictionary dic = bundleContext.getBundle().getHeaders();
                System.err.println( "Test bundle headers: " + explode( dic ) );

                for ( ServiceReference ref : asCollection( bundleContext.getAllServiceReferences( null, null ) ) )
                {
                    System.err.println( "ServiceReference: " + ref );
                }

                for ( ServiceReference ref : asCollection( bundleContext.getAllServiceReferences( null, flt ) ) )
                {
                    System.err.println( "Filtered ServiceReference: " + ref );
                }

                throw new RuntimeException( "Gave up waiting for service " + flt );
            }
            return type.cast( svc );
        }
        catch ( InvalidSyntaxException e )
        {
            throw new IllegalArgumentException( "Invalid filter", e );
        }
        catch ( InterruptedException e )
        {
            throw new RuntimeException( e );
        }
    }


    /*
    * Provides an iterable collection of references, even if the original array is null
    */
    private static Collection<ServiceReference> asCollection( ServiceReference[] references )
    {
        return references != null ? Arrays.asList( references ) : Collections.<ServiceReference>emptyList();
    }

    /**
     * Returns an array of {@link Option} required to install DynamicMemory on any OSGi container.
     *
     * @return
     */
    public static Option[] getDynamicMemoryOptions()
    {
        List<MavenArtifactProvisionOption> mavenOptions = Arrays.asList(
            mavenBundle().groupId( "org.apache.felix" ).artifactId( "org.apache.felix.configadmin" ).version( "1.2.8" ),
            mavenBundle().groupId( "org.ops4j.pax.logging" ).artifactId( "pax-logging-api" ).version( "1.6.2" ),
            mavenBundle().groupId( "org.ops4j.pax.logging" ).artifactId( "pax-logging-service" ).version( "1.6.2" ),
            mavenBundle().groupId( "org.apache.servicemix.bundles" ).artifactId(
                "org.apache.servicemix.bundles.guava" ).version( "09_1" ),
            mavenBundle().groupId( "org.apache.servicemix.bundles" ).artifactId(
                "org.apache.servicemix.bundles.ant" ).version( "1.7.0_5" ),
            mavenBundle().groupId( "org.apache.servicemix.bundles" ).artifactId(
                "org.apache.servicemix.bundles.oro" ).version( "2.0.8_5" ),
            mavenBundle().groupId( "org.apache.servicemix.bundles" ).artifactId(
                "org.apache.servicemix.bundles.aspectj" ).version( "1.6.8_2" ),
            mavenBundle().groupId( "com.dyuproject.protostuff" ).artifactId( "protostuff-uberjar" ).version( "1.0.2" ),
            mavenBundle().groupId( "org.apache.directmemory" ).artifactId( "directmemory-cache" ).version(
                System.getProperty( "direct.memory.version" ) ) );
        List<Option> options = new ArrayList<Option>( mavenOptions );
        if ( Boolean.getBoolean( "osgi.debug" ) )
        {
            options.add( enabledDebuggingOnPort( Integer.getInteger( "osgi.debug.port" ),
                                                 Boolean.getBoolean( "osgi.debug.suspend" ) ) );
        }
        return options.toArray( new Option[options.size()] );
    }

    protected static UrlProvisionOption bundle( final InputStream stream )
        throws IOException
    {
        Store<InputStream> store = StoreFactory.defaultStore();
        return new UrlProvisionOption( store.getLocation( store.store( stream ) ).toURL().toExternalForm() );
    }

    /**
     * Returns an {@link Option} that will enable debugging of the test.
     *
     * @param port
     * @param suspend
     * @return
     */
    public static Option enabledDebuggingOnPort( int port, boolean suspend )
    {
        if ( suspend )
        {
            return vmOption( String.format( JVM_DEBUG_OPTIONS, DO_SUSPEND, port ) );
        }
        else
        {
            return vmOption( String.format( JVM_DEBUG_OPTIONS, DONT_SUSPEND, port ) );
        }
    }
}
