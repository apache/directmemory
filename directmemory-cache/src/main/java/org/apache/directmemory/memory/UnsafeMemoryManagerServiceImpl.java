package org.apache.directmemory.memory;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.directmemory.memory.allocator.Allocator;
import org.apache.directmemory.memory.allocator.LazyUnsafeAllocatorImpl;
import org.apache.directmemory.memory.buffer.MemoryBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UnsafeMemoryManagerServiceImpl<V>
    extends AbstractMemoryManager<V>
    implements MemoryManagerService<V>
{

    protected static final long NEVER_EXPIRES = 0L;

    protected static Logger logger = LoggerFactory.getLogger( MemoryManager.class );

    private final Set<Pointer<V>> pointers = Collections.newSetFromMap( new ConcurrentHashMap<Pointer<V>, Boolean>() );

    private Allocator allocator;

    private long capacity;

    @Override
    public void init( int numberOfBuffers, int size )
    {
        this.capacity = numberOfBuffers * size;
        this.allocator = new LazyUnsafeAllocatorImpl( numberOfBuffers, capacity );
    }

    @Override
    public void close()
        throws IOException
    {
        allocator.close();
        used.set( 0 );
    }

    protected Pointer<V> instanciatePointer( int size, long expiresIn, long expires )
    {
        Pointer<V> p = new PointerImpl<V>( allocator.allocate( size ), 1 );

        p.setExpiration( expires, expiresIn );
        p.setFree( false );
        p.createdNow();

        pointers.add( p );

        return p;
    }

    @Override
    public Pointer<V> store( byte[] payload, long expiresIn )
    {
        if ( capacity - used.get() - payload.length < 0 )
        {
            if ( returnsNullWhenFull() )
            {
                return null;
            }
            else
            {
                throw new BufferOverflowException();
            }
        }

        Pointer<V> p = instanciatePointer( payload.length, expiresIn, NEVER_EXPIRES );
        p.getMemoryBuffer().writeBytes( payload );

        used.addAndGet( payload.length );
        // 2nd version
        // unsafe.copyMemory( srcAddress, address, payload.length );
        return p;
    }

    @Override
    public byte[] retrieve( Pointer<V> pointer )
    {
        final byte[] swp = new byte[(int) pointer.getSize()];

        MemoryBuffer memoryBuffer = pointer.getMemoryBuffer();
        memoryBuffer.readerIndex( 0 );
        memoryBuffer.readBytes( swp );

        return swp;
    }

    @Override
    public Pointer<V> free( Pointer<V> pointer )
    {
        used.addAndGet( -pointer.getSize() );
        allocator.free( pointer.getMemoryBuffer() );
        pointers.remove( pointer );
        pointer.setFree( true );
        return pointer;
    }

    @Override
    public void clear()
    {
        for (Pointer<V> pointer : pointers) {
            free(pointer);
        }
    }

    @Override
    public long capacity()
    {
        return capacity;
    }

    @Override
    public long used()
    {
        return used.get();
    }

    // @Override
    // public long collectExpired()
    // {
    // // TODO Auto-generated method stub
    // return 0;
    // }
    //
    // @Override
    // public void collectLFU()
    // {
    // // TODO Auto-generated method stub
    //
    // }
    //
    // @Override
    // public <T extends V> Pointer<V> allocate( Class<T> type, int size, long expiresIn, long expires )
    // {
    // // TODO Auto-generated method stub
    // return null;
    // }

}
