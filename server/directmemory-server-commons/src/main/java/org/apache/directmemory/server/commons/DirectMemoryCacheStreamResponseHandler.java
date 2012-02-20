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

import org.apache.directmemory.server.commons.DirectMemoryCacheConstants;
import org.apache.directmemory.server.commons.DirectMemoryCacheResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

/**
 * @author Olivier Lamy
 */
public class DirectMemoryCacheStreamResponseHandler
{

    private Logger logger = LoggerFactory.getLogger( getClass() );

    protected DirectMemoryCacheResponse directMemoryCacheResponse = new DirectMemoryCacheResponse();

    private boolean parsingRootElement = false;

    private boolean parsingCacheContentElement = false;

    protected DirectMemoryCacheStreamResponseHandler()
    {
        // no op
    }

    protected DirectMemoryCacheResponse parse( XMLStreamReader streamReader )
        throws XMLStreamException
    {
        while ( streamReader.hasNext() )
        {
            switch ( streamReader.getEventType() )
            {
                case XMLStreamConstants.ATTRIBUTE:
                    break;
                case XMLStreamConstants.START_ELEMENT:
                    handleStartElement( streamReader );
                    break;
                case XMLStreamConstants.END_ELEMENT:
                    handleEndElement( streamReader );
                    break;
                case XMLStreamConstants.START_DOCUMENT:
                    break;
                case XMLStreamConstants.CDATA:
                    handleCData( streamReader );
                    break;
                case XMLStreamConstants.CHARACTERS:
                    break;
                default:
                    logger.debug( "non handled event {}", streamReader.getEventType() );

            }
            streamReader.next();
        }

        return directMemoryCacheResponse;
    }

    private void handleStartElement( XMLStreamReader streamReader )
        throws XMLStreamException
    {
        if ( DirectMemoryCacheConstants.XML_RESPONSE_ROOT_ELEM_NAME.equals( streamReader.getLocalName() ) )
        {
            this.parsingRootElement = true;

            directMemoryCacheResponse.setKey(
                streamReader.getAttributeValue( null, DirectMemoryCacheConstants.KEY_ATT_NAME ) );
            directMemoryCacheResponse.setFound(
                Boolean.valueOf( streamReader.getAttributeValue( null, DirectMemoryCacheConstants.FOUND_ATT_NAME ) ) );
            directMemoryCacheResponse.setUpdated( Boolean.valueOf(
                streamReader.getAttributeValue( null, DirectMemoryCacheConstants.UPDATED_ATT_NAME ) ) );
            return;
        }
        if ( DirectMemoryCacheConstants.CACHE_CONTENT_ELEM_NAME.equals( streamReader.getLocalName() ) )
        {
            this.parsingCacheContentElement = true;
            return;
        }
    }

    private void handleEndElement( XMLStreamReader streamReader )
        throws XMLStreamException
    {
        if ( DirectMemoryCacheConstants.XML_RESPONSE_ROOT_ELEM_NAME.equals( streamReader.getLocalName() ) )
        {
            this.parsingRootElement = false;
            return;
        }
        if ( DirectMemoryCacheConstants.CACHE_CONTENT_ELEM_NAME.equals( streamReader.getLocalName() ) )
        {
            this.parsingCacheContentElement = false;
            return;
        }
    }

    private void handleCData( XMLStreamReader streamReader )
        throws XMLStreamException
    {
        if ( this.parsingCacheContentElement )
        {
            if ( directMemoryCacheResponse.getCacheContent() == null )
            {
                directMemoryCacheResponse.setCacheContent( streamReader.getText().getBytes() );
            }
            else
            {
                byte[] current = directMemoryCacheResponse.getCacheContent();
                byte[] newcontent = streamReader.getText().getBytes();
                byte[] bytes = new byte[current.length + newcontent.length];

                System.arraycopy( current, 0, bytes, 0, current.length - 1 );
                System.arraycopy( newcontent, 0, bytes, current.length, newcontent.length );

                directMemoryCacheResponse.setCacheContent( bytes );
            }
        }
    }
}
