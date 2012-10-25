package org.apache.directmemory.memory;

public class IllegalMemoryPointerException
    extends RuntimeException
{

    private static final long serialVersionUID = -273700198424032755L;

    public IllegalMemoryPointerException()
    {
        super();
    }

    public IllegalMemoryPointerException( String message, Throwable cause )
    {
        super( message, cause );
    }

    public IllegalMemoryPointerException( String message )
    {
        super( message );
    }

    public IllegalMemoryPointerException( Throwable cause )
    {
        super( cause );
    }

}
