package org.apache.directmemory.ehcache;

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

import static java.lang.String.format;

import java.nio.BufferOverflowException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheEntry;
import net.sf.ehcache.CacheException;
import net.sf.ehcache.Ehcache;
import net.sf.ehcache.Element;
import net.sf.ehcache.Status;
import net.sf.ehcache.pool.Pool;
import net.sf.ehcache.pool.PoolableStore;
import net.sf.ehcache.store.AbstractStore;
import net.sf.ehcache.store.ElementValueComparator;
import net.sf.ehcache.store.Policy;
import net.sf.ehcache.store.TierableStore;
import net.sf.ehcache.store.disk.StoreUpdateException;
import net.sf.ehcache.writer.CacheWriterManager;

import org.apache.directmemory.cache.CacheServiceImpl;
import org.apache.directmemory.measures.Ram;
import org.apache.directmemory.memory.OffHeapMemoryBuffer;
import org.apache.directmemory.memory.Pointer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DirectMemoryStore
    extends AbstractStore
    implements TierableStore, PoolableStore
{

    private static Logger logger = LoggerFactory.getLogger( CacheServiceImpl.class );

    public static final int DEFAULT_NUMBER_BYTE_BUFFERS = 64;

    public static final int DEFAULT_BUFFER_SIZE = Ram.Mb( 40 );

    private List<ReentrantLock> bufferLocks;

    DirectMemoryCache<Object, Element> directMemoryCache;

    public DirectMemoryStore( Ehcache cache, Pool<PoolableStore> offHeapPool )
    {
        this( cache, offHeapPool, false );
    }

    public DirectMemoryStore( Ehcache cache, Pool<PoolableStore> offHeapPool, boolean doNotifications )
    {
        long offHeapSizeBytes;
        if ( cache == null || cache.getCacheConfiguration() == null )
        {
            offHeapSizeBytes = Ram.Mb( 64 );
        }
        else
        {
            offHeapSizeBytes = cache.getCacheConfiguration().getMaxMemoryOffHeapInBytes();
        }
        init( offHeapSizeBytes );
    }

    public DirectMemoryStore( long offHeapSizeBytes )
    {
        init( offHeapSizeBytes );
    }

    private void init( long offHeapSizeBytes )
    {
        logger.info( "	  ___   __  __ _   _                 ____  _                 " );
        logger.info( "	 / _ \\ / _|/ _| | | | ___  __ _ _ __/ ___|| |_ ___  _ __ ___ " );
        logger.info( "	| | | | |_| |_| |_| |/ _ \\/ _` | '_ \\___ \\| __/ _ \\| '__/ _ \\" );
        logger.info( "	| |_| |  _|  _|  _  |  __/ (_| | |_) |__) | || (_) | | |  __/" );
        logger.info( "	 \\___/|_| |_| |_| |_|\\___|\\__,_| .__/____/ \\__\\___/|_|  \\___|" );
        logger.info( "	                               |_|                           " );

        logger.info( "default buffer size = " + DEFAULT_BUFFER_SIZE );
        logger.info( "off heap size = " + offHeapSizeBytes );
        int numberOfBuffers = (int) ( offHeapSizeBytes / DEFAULT_BUFFER_SIZE );
        numberOfBuffers = DEFAULT_NUMBER_BYTE_BUFFERS;
        logger.info( "no of buffers = " + numberOfBuffers );

        this.bufferLocks = new ArrayList<ReentrantLock>( numberOfBuffers );
        for ( int i = 0; i < numberOfBuffers; i++ )
        {
            this.bufferLocks.add( new ReentrantLock() );
        }

        directMemoryCache = new DirectMemoryCache<Object, Element>( numberOfBuffers,
                                                                    (int) ( offHeapSizeBytes / numberOfBuffers ) );
    }

    @Override
    public void unpinAll()
    {
        //no operation

    }

    @Override
    public boolean isPinned( Object key )
    {
        return false;
    }

    @Override
    public void setPinned( Object key, boolean pinned )
    {
        //no operation

    }

    @Override
    public boolean put( Element element )
        throws CacheException
    {
        Pointer<Element> pointer = null;
        try
        {
            pointer = directMemoryCache.put( element.getObjectKey(), element );
        }
        catch ( BufferOverflowException boe )
        {
            dump();
            throw new CacheException( "DirectMemory OffHeap Memory Exceeded", boe );
        }
        return null == pointer ? false : true;
    }

    @Override
    public boolean putWithWriter( Element element, CacheWriterManager writerManager )
        throws CacheException
    {
        boolean newPut = put( element );
        if ( writerManager != null )
        {
            try
            {
                writerManager.put( element );
            }
            catch ( RuntimeException e )
            {
                throw new StoreUpdateException( e, !newPut );
            }
        }
        return newPut;
    }

    @Override
    public Element get( Object key )
    {
        return directMemoryCache.retrieve( key );
    }

    @Override
    public Element getQuiet( Object key )
    {
        return get( key );
    }

    @Override
    public List<Object> getKeys()
    {
        return new ArrayList<Object>( directMemoryCache.getKeys() );
    }

    @Override
    public Element remove( Object key )
    {
        Element element = get( key );
        directMemoryCache.free( key );
        return element;

    }

    @Override
    public Element removeWithWriter( Object key, CacheWriterManager writerManager )
        throws CacheException
    {
        Element removed = remove( key );
        if ( writerManager != null )
        {
            writerManager.remove( new CacheEntry( key, removed ) );
        }
        return removed;
    }

    @Override
    public void removeAll()
        throws CacheException
    {
        directMemoryCache.clear();
    }

    @Override
    public Element putIfAbsent( Element element )
        throws NullPointerException
    {
        Element returnElement = get( element.getObjectKey() );
        if ( null == returnElement )
        {
            put( element );
            returnElement = element;
        }
        return returnElement;
    }

    @Override
    public Element removeElement( Element element, ElementValueComparator comparator )
        throws NullPointerException
    {
        if ( element == null || element.getObjectKey() == null )
        {
            return null;
        }
        Pointer<Element> pointer = directMemoryCache.getPointer( element.getObjectKey() );
        if ( pointer == null )
        {
            return null;
        }

        Lock lock = bufferLocks.get( pointer.getBufferNumber() );
        lock.lock();
        try
        {
            Element toRemove = directMemoryCache.retrieve( element.getObjectKey() );
            if ( comparator.equals( element, toRemove ) )
            {
                directMemoryCache.free( element.getObjectKey() );
                return toRemove;
            }
            else
            {
                return null;
            }
        }
        finally
        {
            lock.unlock();
        }
    }

    @Override
    public boolean replace( Element old, Element element, ElementValueComparator comparator )
        throws NullPointerException, IllegalArgumentException
    {
        if ( element == null || element.getObjectKey() == null )
        {
            return false;
        }
        Pointer<Element> pointer = directMemoryCache.getPointer( element.getObjectKey() );
        if ( pointer == null )
        {
            return false;
        }

        Lock lock = bufferLocks.get( pointer.getBufferNumber() );
        lock.lock();
        try
        {
            Element toUpdate = directMemoryCache.retrieve( element.getObjectKey() );
            if ( comparator.equals( old, toUpdate ) )
            {
                directMemoryCache.put( element.getObjectKey(), element );
                return true;
            }
            else
            {
                return false;
            }
        }
        catch ( BufferOverflowException boe )
        {
            dump();
            throw new CacheException( "DirectMemory OffHeap Memory Exceeded", boe );
        }
        finally
        {
            lock.unlock();
        }
    }

    @Override
    public Element replace( Element element )
        throws NullPointerException
    {
        if ( element == null || element.getObjectKey() == null )
        {
            return null;
        }
        Pointer<Element> pointer = directMemoryCache.getPointer( element.getObjectKey() );
        if ( pointer == null )
        {
            return null;
        }

        Lock lock = bufferLocks.get( pointer.getBufferNumber() );
        lock.lock();
        try
        {
            Element toUpdate = directMemoryCache.retrieve( element.getObjectKey() );
            if ( null != toUpdate )
            {
                directMemoryCache.put( element.getObjectKey(), element );
                return toUpdate;
            }
            else
            {
                return null;
            }
        }
        catch ( BufferOverflowException boe )
        {
            dump();
            throw new CacheException( "DirectMemory OffHeap Memory Exceeded", boe );
        }
        finally
        {
            lock.unlock();
        }
    }

    @Override
    public synchronized void dispose()
    {
        flush();
    }

    @Override
    public int getSize()
    {
        return getOffHeapSize();
    }

    @Override
    public int getInMemorySize()
    {
        //no operation
        return 0;
    }

    @Override
    public int getOffHeapSize()
    {
        long size = directMemoryCache.size();
        if ( size > Integer.MAX_VALUE )
        {
            return Integer.MAX_VALUE;
        }
        else
        {
            return (int) size;
        }
    }

    @Override
    public int getOnDiskSize()
    {
        //no operation
        return 0;
    }

    @Override
    public int getTerracottaClusteredSize()
    {
        //no operation
        return 0;
    }

    @Override
    public long getInMemorySizeInBytes()
    {
        //no operation
        return 0;
    }

    @Override
    public long getOffHeapSizeInBytes()
    {
        return directMemoryCache.sizeInBytes();
    }

    @Override
    public long getOnDiskSizeInBytes()
    {
        //no operation
        return 0;
    }

    @Override
    public Status getStatus()
    {
        //no operation
        return null;
    }

    @Override
    public boolean containsKey( Object key )
    {
        return containsKeyOffHeap( key );
    }

    @Override
    public boolean containsKeyOnDisk( Object key )
    {
        //no operation
        return false;
    }

    @Override
    public boolean containsKeyOffHeap( Object key )
    {
        return directMemoryCache.containsKey( key );
    }

    @Override
    public boolean containsKeyInMemory( Object key )
    {
        //no operation
        return false;
    }

    @Override
    public void expireElements()
    {
        //no operation

    }

    @Override
    public void flush()
    {
        directMemoryCache.clear();
    }

    @Override
    public boolean bufferFull()
    {
        //never backs up/ no buffer used.
        return false;
    }

    @Override
    public Policy getInMemoryEvictionPolicy()
    {
        //no operation
        return null;
    }

    @Override
    public void setInMemoryEvictionPolicy( Policy policy )
    {
        //no operation

    }

    @Override
    public Object getInternalContext()
    {
        //no operation
        return null;
    }

    @Override
    public Object getMBean()
    {
        //no operation
        return null;
    }

    @Override
    public boolean evictFromOnHeap( int count, long size )
    {
        //no operation
        return false;
    }

    @Override
    public boolean evictFromOnDisk( int count, long size )
    {
        //no operation
        return false;
    }

    @Override
    public float getApproximateDiskHitRate()
    {
        //no operation
        return 0;
    }

    @Override
    public float getApproximateDiskMissRate()
    {
        //no operation
        return 0;
    }

    @Override
    public long getApproximateDiskCountSize()
    {
        //no operation
        return 0;
    }

    @Override
    public long getApproximateDiskByteSize()
    {
        //no operation
        return 0;
    }

    @Override
    public float getApproximateHeapHitRate()
    {
        //no operation
        return 0;
    }

    @Override
    public float getApproximateHeapMissRate()
    {
        //no operation
        return 0;
    }

    @Override
    public long getApproximateHeapCountSize()
    {
        //no operation
        return 0;
    }

    @Override
    public long getApproximateHeapByteSize()
    {
        //no operation
        return 0;
    }

    @Override
    public void fill( Element e )
    {
        put( e );
    }

    @Override
    public boolean removeIfTierNotPinned( Object key )
    {
        //no operation
        return false;
    }

    @Override
    public void removeNoReturn( Object key )
    {
        //no operation

    }

    public void dump()
    {
        directMemoryCache.dump();
    }

    public void dumpTotal()
    {
        long capacity = 0;
        long used = 0;
        for ( OffHeapMemoryBuffer<Element> buffer : directMemoryCache.getMemoryManager().getBuffers() )
        {
            capacity += buffer.capacity();
            used += buffer.used();
        }
        logger.info( "***Totals***************************************" );
        logger.info( format( "off-heap - # buffers: \t%1d", directMemoryCache.getMemoryManager().getBuffers().size() ) );
        logger.info( format( "off-heap - allocated: \t%1s", Ram.inMb( capacity ) ) );
        logger.info( format( "off-heap - used:      \t%1s", Ram.inMb( used ) ) );
        logger.info( format( "heap     - max: \t%1s", Ram.inMb( Runtime.getRuntime().maxMemory() ) ) );
        logger.info( format( "heap     - allocated: \t%1s", Ram.inMb( Runtime.getRuntime().totalMemory() ) ) );
        logger.info( format( "heap     - free : \t%1s", Ram.inMb( Runtime.getRuntime().freeMemory() ) ) );
        logger.info( "************************************************" );
    }
}
