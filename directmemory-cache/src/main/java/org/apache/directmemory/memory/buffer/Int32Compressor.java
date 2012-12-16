package org.apache.directmemory.memory.buffer;

public final class Int32Compressor
{

    private Int32Compressor()
    {
    }

    static final byte INT32_FULL = 1;

    static final byte INT32_COMPRESSED_SINGLE = 2;

    static final byte INT32_COMPRESSED_DOUBLE = 3;

    static final byte INT32_COMPRESSED_TRIPPLE = 4;

    static final int INT32_MAX_SINGLE = 0x7F;

    static final int INT32_MIN_SINGLE = ~INT32_MAX_SINGLE + 1;

    static final int INT32_MAX_DOUBLE = 0x7FFF;

    static final int INT32_MIN_DOUBLE = ~INT32_MAX_DOUBLE + 1;

    static final int INT32_MAX_TRIPPLE = 0x7FFFFF;

    static final int INT32_MIN_TRIPPLE = ~INT32_MAX_TRIPPLE + 1;

    public static void writeInt32( int value, WritableMemoryBuffer memoryBuffer )
    {
        if ( value >= INT32_MIN_SINGLE && value <= INT32_MAX_SINGLE )
        {
            value = value < 0 ? ( ~value + 1 ) | ( 1 << 7 ) : value;
            memoryBuffer.writeByte( INT32_COMPRESSED_SINGLE );
            memoryBuffer.writeByte( (byte) value );
        }
        else if ( value >= INT32_MIN_DOUBLE && value <= INT32_MAX_DOUBLE )
        {
            value = value < 0 ? ( ~value + 1 ) | ( 1 << 15 ) : value;
            memoryBuffer.writeByte( INT32_COMPRESSED_DOUBLE );
            memoryBuffer.writeByte( (byte) ( value >> 8 ) );
            memoryBuffer.writeByte( (byte) value );
        }
        else if ( value >= INT32_MIN_TRIPPLE && value <= INT32_MAX_TRIPPLE )
        {
            value = value < 0 ? ( ~value + 1 ) | ( 1 << 23 ) : value;
            memoryBuffer.writeByte( INT32_COMPRESSED_TRIPPLE );
            memoryBuffer.writeByte( (byte) ( value >> 16 ) );
            memoryBuffer.writeByte( (byte) ( value >> 8 ) );
            memoryBuffer.writeByte( (byte) value );
        }
        else
        {
            memoryBuffer.writeByte( INT32_FULL );
            memoryBuffer.writeByte( (byte) ( value >> 24 ) );
            memoryBuffer.writeByte( (byte) ( value >> 16 ) );
            memoryBuffer.writeByte( (byte) ( value >> 8 ) );
            memoryBuffer.writeByte( (byte) value );
        }
    }

    public static int readInt32( ReadableMemoryBuffer memoryBuffer )
    {
        byte type = memoryBuffer.readByte();
        switch ( type )
        {
            case INT32_COMPRESSED_SINGLE:
            {
                int data = memoryBuffer.readByte() & 0xFF;
                return ( ( data >> 7 ) & 1 ) == 1 ? ~( data ^ ( 1 << 7 ) ) + 1 : data;
            }

            case INT32_COMPRESSED_DOUBLE:
            {
                int data = ( ( memoryBuffer.readByte() & 0xFF ) << 8 ) | ( memoryBuffer.readByte() & 0xFF );
                return ( ( data >> 15 ) & 1 ) == 1 ? ~( data ^ ( 1 << 15 ) ) + 1 : data;
            }

            case INT32_COMPRESSED_TRIPPLE:
            {
                int data =
                    ( ( memoryBuffer.readByte() & 0xFF ) << 16 ) | ( ( memoryBuffer.readByte() & 0xFF ) << 8 )
                        | ( memoryBuffer.readByte() & 0xFF );
                return ( ( data >> 23 ) & 1 ) == 1 ? ~( data ^ ( 1 << 23 ) ) + 1 : data;
            }
        }

        return ( ( memoryBuffer.readByte() & 0xFF ) << 24 ) | ( ( memoryBuffer.readByte() & 0xFF ) << 16 )
            | ( ( memoryBuffer.readByte() & 0xFF ) << 8 ) | ( memoryBuffer.readByte() & 0xFF );
    }

}
