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

    public String generateXmlRequest( DirectMemoryCacheRequest request )
        throws DirectMemoryCacheException
    {
        try
        {
            StringWriter stringWriter = new StringWriter();
            XMLStreamWriter xmlStreamWriter = xmlOutputFactory.createXMLStreamWriter( stringWriter );
            xmlStreamWriter.writeStartDocument( "1.0" );

            xmlStreamWriter.writeStartElement( DirectMemoryCacheConstants.XML_REQUEST_ROOT_ELEM_NAME );

            xmlStreamWriter.writeAttribute( DirectMemoryCacheConstants.KEY_ATT_NAME, request.getKey() );
            xmlStreamWriter.writeAttribute( DirectMemoryCacheConstants.PUT_ATT_NAME,
                                            Boolean.toString( request.isUpdate() ) );
            xmlStreamWriter.writeAttribute( DirectMemoryCacheConstants.EXPIRES_IN_ATT_NAME,
                                            Integer.toString( request.getExpiresIn() ) );

            if ( request.isUpdate() )
            {
                // FIXME take care of NPE
                // cache content generation
                Serializer serializer = request.getSerializer();
                // if no Object users are able to pass a string content
                byte[] bytes = request.getObject() != null
                    ? request.getSerializer().serialize( request.getObject() )
                    : request.getCacheContent();
                xmlStreamWriter.writeStartElement( DirectMemoryCacheConstants.CACHE_CONTENT_ELEM_NAME );
                xmlStreamWriter.writeCData( new String( bytes ) );// charset ?
                xmlStreamWriter.writeEndElement();
            }

            xmlStreamWriter.writeEndElement();

            xmlStreamWriter.writeEndDocument();

            return stringWriter.toString();
        }
        catch ( IOException e )
        {
            throw new DirectMemoryCacheException( e.getMessage(), e );
        }
        catch ( XMLStreamException e )
        {
            throw new DirectMemoryCacheException( e.getMessage(), e );
        }

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

            g.writeObjectFieldStart( DirectMemoryCacheConstants.XML_REQUEST_ROOT_ELEM_NAME );

            g.writeStringField( DirectMemoryCacheConstants.KEY_ATT_NAME, request.getKey() );

            g.writeBooleanField( DirectMemoryCacheConstants.PUT_ATT_NAME, request.isUpdate() );

            g.writeNumberField( DirectMemoryCacheConstants.EXPIRES_IN_ATT_NAME, request.getExpiresIn() );

            // FIXME take care of NPE
            // cache content generation
            Serializer serializer = request.getSerializer();
            // if no Object users are able to pass a string content
            byte[] bytes = request.getObject() != null
                ? request.getSerializer().serialize( request.getObject() )
                : request.getCacheContent();

            g.writeFieldName( DirectMemoryCacheConstants.CACHE_CONTENT_ELEM_NAME );
            g.writeBinary( bytes );

            if ( serializer != null )
            {
                g.writeStringField( DirectMemoryCacheConstants.SERIALIZER_FIELD_NAME, serializer.getClass().getName() );
            }

            g.writeEndObject();
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

            g.writeObjectFieldStart( DirectMemoryCacheConstants.XML_RESPONSE_ROOT_ELEM_NAME );

            g.writeBooleanField( DirectMemoryCacheConstants.FOUND_ATT_NAME, response.isFound() );

            g.writeBooleanField( DirectMemoryCacheConstants.UPDATED_ATT_NAME, response.isUpdated() );

            g.writeStringField( DirectMemoryCacheConstants.KEY_ATT_NAME, response.getKey() );

            if ( response.getCacheContent() != null && response.getCacheContent().length > 0 )
            {
                g.writeFieldName( DirectMemoryCacheConstants.CACHE_CONTENT_ELEM_NAME );
                g.writeBinary( response.getCacheContent() );
            }
            g.writeEndObject();
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
