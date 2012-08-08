package org.apache.directmemory.conf;

public abstract class DefaultConfiguration implements ConfigurationService
{
    private int numberOfBuffers = 1;

    private int initialCapacity = 100000;

    private int ramMegaBytes = 1;

    private int concurrencyLevel = 4;

    private long disposalTime = 10L;

    @Override
    public int getNumberOfBuffers()
    {
        return numberOfBuffers;
    }

    @Override
    public int getInitialCapacity()
    {
        return initialCapacity;
    }

    @Override
    public int getRamMegaBytes()
    {
        return ramMegaBytes;
    }

    @Override
    public int getConcurrencyLevel()
    {
        return concurrencyLevel;
    }

    @Override
    public long getDisposalTime()
    {
        return disposalTime;
    }
    
    @Override
    public void setNumberOfBuffers( int numberOfBuffers )
    {
        this.numberOfBuffers = numberOfBuffers;
    }

    @Override
    public void setInitialCapacity( int initialCapacity )
    {
        this.initialCapacity = initialCapacity;
    }

    @Override
    public void setRamMegaBytes( int ramMegaBytes )
    {
        this.ramMegaBytes = ramMegaBytes;
    }

    @Override
    public void setConcurrencyLevel( int concurrencyLevel )
    {
        this.concurrencyLevel = concurrencyLevel;
    }

    @Override
    public void setDisposalTime( long disposalTime )
    {
        this.disposalTime = disposalTime;
    }

}
