package org.apache.directmemory.memory.allocator;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;

import static com.google.common.base.Preconditions.checkArgument;

public class DirectByteBufferUtils
{

    public static void destroyDirectByteBuffer( final ByteBuffer buffer )
        throws IllegalArgumentException, IllegalAccessException, InvocationTargetException, SecurityException,
        NoSuchMethodException
    {

        checkArgument( buffer.isDirect(), "toBeDestroyed isn't direct!" );

        Method cleanerMethod = buffer.getClass().getMethod( "cleaner" );
        cleanerMethod.setAccessible( true );
        Object cleaner = cleanerMethod.invoke( buffer );
        Method cleanMethod = cleaner.getClass().getMethod( "clean" );
        cleanMethod.setAccessible( true );
        cleanMethod.invoke( cleaner );

    }
    
}
