package org.apache.directmemory.memory;

public class DefaultMemoryManagerServiceTest
    extends AbstractMemoryManagerServiceTest
{

    @Override
    protected MemoryManagerService<Object> instanciateMemoryManagerService( int bufferSize )
    {
        final MemoryManagerService<Object> mms = new MemoryManagerServiceImpl<Object>();
        mms.init( 1, bufferSize );
        return mms;
    }

}
