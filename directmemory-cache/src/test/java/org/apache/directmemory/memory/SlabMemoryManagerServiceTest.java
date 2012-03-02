package org.apache.directmemory.memory;

import java.util.Collection;
import java.util.HashSet;

import org.apache.directmemory.memory.allocator.ByteBufferAllocator;
import org.apache.directmemory.memory.allocator.FixedSizeByteBufferAllocatorImpl;
import org.apache.directmemory.memory.allocator.SlabByteBufferAllocatorImpl;
import org.junit.Test;

public class SlabMemoryManagerServiceTest
    extends AbstractMemoryManagerServiceTest
{

    @Override
    protected MemoryManagerService<Object> instanciateMemoryManagerService( int bufferSize )
    {
        final MemoryManagerService<Object> mms = new MemoryManagerServiceImpl<Object>() {

            @Override
            protected ByteBufferAllocator instanciateByteBufferAllocator( int allocatorNumber, int size )
            {
                Collection<FixedSizeByteBufferAllocatorImpl> slabs = new HashSet<FixedSizeByteBufferAllocatorImpl>();
                
                slabs.add( new FixedSizeByteBufferAllocatorImpl(0, size, SMALL_PAYLOAD_LENGTH / 2, 1) );
                slabs.add( new FixedSizeByteBufferAllocatorImpl(1, size, SMALL_PAYLOAD_LENGTH, 1) );
                
                final SlabByteBufferAllocatorImpl allocator = new SlabByteBufferAllocatorImpl( allocatorNumber, slabs, false );
                
                return allocator;
            }
            
        };
        mms.init( 1, bufferSize );
        return mms;
    }

    @Override
    @Test
    public void testFullFillAndFreeAndClearBuffer()
    {
        
    }
}
