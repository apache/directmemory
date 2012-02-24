package org.apache.directmemory;

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

import static java.lang.String.format;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Formatter;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

import org.apache.directmemory.cache.CacheService;
import org.apache.directmemory.memory.MemoryManagerService;
import org.apache.directmemory.memory.Pointer;
import org.apache.directmemory.serialization.Serializer;

final class CacheConfiguratorImpl<K, V>
    implements CacheConfigurator<K, V>
{

    /**
     * The default head when reporting an errors list.
     */
    private static final String HEADING = "Apache DirectMemory creation errors:%n%n";

    private static int DEFAULT_INITIAL_CAPACITY = 100000;

    private static int DEFAULT_CONCURRENCY_LEVEL = 4;

    private final List<ErrorMessage> errors = new LinkedList<ErrorMessage>();

    int numberOfBuffers;

    int size;

    MemoryManagerService<V> memoryManager;

    ConcurrentMap<String, Pointer<V>> map;

    Serializer serializer;

    @Override
    public MemoryUnitDimensionBuilder allocateMemoryOfSize( double size )
    {
        checkInput( size > 0, "Input value %s is not a valid value to express a memory space", size );
        return new DefaultMemoryUnitDimensionBuilder( this, size );
    }

    @Override
    public SizeBuilder numberOfBuffers()
    {
        // TODO
        return null;
    }

    @Override
    public TimeMeasureBuilder scheduleDisposalEvery( long time )
    {
        checkInput( time > 0, "Input value %s is not a valid value to express a time measure", time );
        // TODO
        return null;
    }

    private void checkInput( boolean expression, String errorMessageTemplate, Object... errorMessageArgs )
    {
        if ( !expression )
        {
            Exception exception = new Exception();

            /*
             * 0 is this method
             * 1 is the calling method
             * 2 is the configure() method
             * 3 is the DirectMemory class
             * 4 is the implementing class
             */
            StackTraceElement methodElement = exception.getStackTrace()[1];
            StackTraceElement fileElement = exception.getStackTrace()[4];
            if ( AbstractCacheConfiguration.class.getName().equals( fileElement.getClassName() ) )
            {
                /*
                 * 5 is the DirectMemory class
                 * 6 is the implementing class
                 */
                fileElement = exception.getStackTrace()[6];
            }

            String enhancedErrorMessage = format( "%s - [CacheConfiguration#%s() - %s:%s]",
                                                  errorMessageTemplate,
                                                  methodElement.getMethodName(),
                                                  fileElement.getFileName(),
                                                  fileElement.getLineNumber() );

            errors.add( new ErrorMessage( enhancedErrorMessage, errorMessageArgs ) );
        }
    }

    public CacheService<K, V> createInstance()
    {
        if ( !errors.isEmpty() )
        {
            Formatter fmt = new Formatter().format( HEADING );
            int index = 1;

            for ( ErrorMessage errorMessage : errors )
            {
                fmt.format( "%s) %s%n", index++, errorMessage.getMessage() );

                Throwable cause = errorMessage.getCause();
                if ( cause != null )
                {
                    StringWriter writer = new StringWriter();
                    cause.printStackTrace( new PrintWriter( writer ) );
                    fmt.format( "Caused by: %s", writer.getBuffer() );
                }

                fmt.format( "%n" );
            }

            if ( errors.size() == 1 )
            {
                fmt.format( "1 error" );
            }
            else
            {
                fmt.format( "%s errors", errors.size() );
            }

            throw new DirectMemoryConfigurationException( fmt.toString() );
        }

        // TODO needs to be completed
        return null;
    }

}
