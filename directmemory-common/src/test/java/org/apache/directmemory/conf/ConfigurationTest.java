package org.apache.directmemory.conf;

import static org.junit.Assert.*;

import org.junit.Test;

public class ConfigurationTest
{
    @Test
    public void testBaseConfiguration()
    {
        assertTrue( Configuration.getNumberOfBuffers() > 0 );
        assertTrue( Configuration.getInitialCapacity() > 0 );
        assertTrue( Configuration.getRamMegaBytes() > 0 );
        assertTrue( Configuration.getConcurrencyLevel() > 0 );
        assertTrue( Configuration.getDisposalTime() > 0L );
    }
    
    @Test
    public void testYamlConfiguration()
    {
        Configuration.configureFromYaml();
        assertTrue( Configuration.getNumberOfBuffers() > 0 );
        assertTrue( Configuration.getInitialCapacity() > 0 );
        assertTrue( Configuration.getRamMegaBytes() > 0 );
        assertTrue( Configuration.getConcurrencyLevel() > 0 );
        assertTrue( Configuration.getDisposalTime() > 0L );
    }
}
