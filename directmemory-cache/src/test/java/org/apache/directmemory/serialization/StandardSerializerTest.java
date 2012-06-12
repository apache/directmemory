/**
 *
 */
package org.apache.directmemory.serialization;

import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;

import static org.junit.Assert.*;

/**
 *
 */
public class StandardSerializerTest
{

    private static Serializer serializer;

    private static final Random r = new Random();

    @BeforeClass
    public static void init()
    {
        serializer = new StandardSerializer();
    }

    @Test
    public void validateBooleanSerialization()
        throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException
    {
        byte[] payload = serializer.serialize( true );
        boolean res = serializer.deserialize( payload, Boolean.class );
        assertEquals( true, res );
    }

    @Test
    public void validateBooleanArraySerialization()
        throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException
    {
        byte[] payload = serializer.serialize( new boolean[]{ true } );
        boolean[] res = serializer.deserialize( payload, boolean[].class );
        assertEquals( 1, res.length );
        assertTrue( res[0] );
    }

    @Test
    public void validateByteSerialization()
        throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException
    {
        byte[] payload = serializer.serialize( (byte) 127 );
        byte res = serializer.deserialize( payload, Byte.class );
        assertEquals( (byte) 127, res );
    }

    @Test
    public void validateByteArraySerialization()
        throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException
    {
        byte[] value = new byte[1024];
        r.nextBytes( value );
        byte[] payload = serializer.serialize( value );
        byte[] res = serializer.deserialize( payload, byte[].class );
        assertArrayEquals( value, res );
    }

    @Test
    public void validateCharacterSerialization()
        throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException
    {
        byte[] payload = serializer.serialize( 'z' );
        char res = serializer.deserialize( payload, Character.class );
        assertEquals( 'z', res );
    }

    @Test
    public void validateCharacterArraySerialization()
        throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException
    {
        char[] value = new char[]{ 'a', 'z', 'x', ' ', '-', '-' };
        byte[] payload = serializer.serialize( value );
        char[] res = serializer.deserialize( payload, char[].class );
        assertArrayEquals( value, res );
    }

    @Test
    public void validateDoubleSerialization()
        throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException
    {
        byte[] payload = serializer.serialize( 1.2d );
        double res = serializer.deserialize( payload, Double.class );
        assertEquals( 1.2d, res, 0.0d );
    }

    @Test
    public void validateDoubleArraySerialization()
        throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException
    {
        double[] value = new double[]{ 1.1d, 3.1d, 0.1d };
        byte[] payload = serializer.serialize( value );
        double[] res = serializer.deserialize( payload, double[].class );
        assertArrayEquals( value, res, 0.0d );
    }

    @Test
    public void validateFloatSerialization()
        throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException
    {
        byte[] payload = serializer.serialize( 1.2f );
        float res = serializer.deserialize( payload, Float.class );
        assertEquals( 1.2f, res, 0.0d );
    }

    @Test
    public void validateFloatArraySerialization()
        throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException
    {
        float[] value = new float[]{ 1.1f, 4.05f, 55.5f };
        byte[] payload = serializer.serialize( value );
        float[] res = serializer.deserialize( payload, float[].class );
        assertArrayEquals( value, res, 0.0f );
    }

    @Test
    public void validateIntegerSerialization()
        throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException
    {
        byte[] payload = serializer.serialize( 1 );
        int res = serializer.deserialize( payload, Integer.class );
        assertEquals( 1, res );
    }

    @Test
    public void validateIntegerArraySerialization()
        throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException
    {
        int[] value = new int[]{ 3, 1, -1 };
        byte[] payload = serializer.serialize( value );
        int[] res = serializer.deserialize( payload, int[].class );
        assertArrayEquals( value, res );
    }

    @Test
    public void validateLongSerialization()
        throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException
    {
        byte[] payload = serializer.serialize( 1L );
        long res = serializer.deserialize( payload, Long.class );
        assertEquals( 1, res );
    }

    @Test
    public void validateLongArraySerialization()
        throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException
    {
        long[] value = new long[]{ 1l, 3l, 1212121212121l };
        byte[] payload = serializer.serialize( value );
        long[] res = serializer.deserialize( payload, long[].class );
        assertArrayEquals( value, res );
    }

    @Test
    public void validateShortSerialization()
        throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException
    {
        byte[] payload = serializer.serialize( (short) 1234 );
        short res = serializer.deserialize( payload, Short.class );
        assertEquals( 1234, res );
    }

    @Test
    public void validateShortArraySerialization()
        throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException
    {
        short[] value = new short[]{ 1, -1, 4, 32767 };
        byte[] payload = serializer.serialize( value );
        short[] res = serializer.deserialize( payload, short[].class );
        assertArrayEquals( value, res );
    }

    @Test
    public void validateStringSerialization()
        throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException
    {
        String value = "a sample string to serialize";
        byte[] payload = serializer.serialize( value );
        String res = serializer.deserialize( payload, String.class );
        assertEquals( value, res );
    }

    @Test
    public void validateStringArraySerialization()
        throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException
    {
        String[] value = new String[]{ "String1", "", "String2" };
        byte[] payload = serializer.serialize( value );
        String[] res = serializer.deserialize( payload, String[].class );
        assertArrayEquals( value, res );
    }
}
