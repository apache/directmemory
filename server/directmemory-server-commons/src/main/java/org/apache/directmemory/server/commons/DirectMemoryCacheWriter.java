package org.apache.directmemory.server.commons;
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
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.IOException;
import java.io.StringWriter;

/**
 * @author Olivier Lamy
 */
public class DirectMemoryCacheWriter
{
    private XMLOutputFactory xmlOutputFactory;

    private JsonFactory jsonFactory;

    private static final DirectMemoryCacheWriter INSTANCE = new DirectMemoryCacheWriter();

    private Logger log = LoggerFactory.getLogger( getClass() );

    public static DirectMemoryCacheWriter instance()
    {
        return INSTANCE;
    }

    private DirectMemoryCacheWriter()
    {
        this.xmlOutputFactory = XMLOutputFactory.newInstance();
        this.xmlOutputFactory.setProperty( XMLOutputFactory.IS_REPAIRING_NAMESPACES, Boolean.FALSE );

        this.jsonFactory = new JsonFactory();
    }

    public String generateJsonRequest( DirectMemoryCacheRequest request )
        throws DirectMemoryCacheException
    {
        // TODO configure a minimum size for the writer
        StringWriter stringWriter = new StringWriter();

        try
        {
            JsonGenerator g = this.jsonFactory.createJsonGenerator( stringWriter );

            g.writeStartObject();

            g.writeStringField( DirectMemoryCacheConstants.KEY_FIELD_NAME, request.getKey() );

            g.writeBooleanField( DirectMemoryCacheConstants.PUT_FIELD_NAME, request.isUpdate() );

            g.writeNumberField( DirectMemoryCacheConstants.EXPIRES_IN_FIELD_NAME, request.getExpiresIn() );

            // FIXME take care of NPE
            // cache content generation
            Serializer serializer = request.getSerializer();
            // if no Object users are able to pass a string content
            byte[] bytes = request.getObject() != null
                ? request.getSerializer().serialize( request.getObject() )
                : request.getCacheContent();

            g.writeFieldName( DirectMemoryCacheConstants.CACHE_CONTENT_FIELD_NAME );
            g.writeBinary( bytes );

            if ( serializer != null )
            {
                g.writeStringField( DirectMemoryCacheConstants.SERIALIZER_FIELD_NAME, serializer.getClass().getName() );
            }

            g.writeEndObject();
            g.close();
        }
        catch ( IOException e )
        {
            throw new DirectMemoryCacheException( e.getMessage(), e );
        }

        return stringWriter.toString();
    }

    public String generateJsonResponse( DirectMemoryCacheResponse response )
        throws DirectMemoryCacheException
    {

        // TODO configure a minimum size for the writer
        StringWriter stringWriter = new StringWriter();

        try
        {

            JsonGenerator g = this.jsonFactory.createJsonGenerator( stringWriter );

            g.writeStartObject();

            g.writeBooleanField( DirectMemoryCacheConstants.FOUND_FIELD_NAME, response.isFound() );

            g.writeBooleanField( DirectMemoryCacheConstants.UPDATED_FIELD_NAME, response.isUpdated() );

            g.writeStringField( DirectMemoryCacheConstants.KEY_FIELD_NAME, response.getKey() );

            if ( response.getCacheContent() != null && response.getCacheContent().length > 0 )
            {
                g.writeFieldName( DirectMemoryCacheConstants.CACHE_CONTENT_FIELD_NAME );
                g.writeBinary( response.getCacheContent() );
            }

            g.writeEndObject();
            g.close();

        }
        catch ( IOException e )
        {
            throw new DirectMemoryCacheException( e.getMessage(), e );
        }

        return stringWriter.toString();

    }
}
