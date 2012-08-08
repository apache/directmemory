package org.apache.directmemory.conf;

public interface ConfigurationService
{
    public int getNumberOfBuffers();

    public int getInitialCapacity();

    public int getRamMegaBytes();

    public int getConcurrencyLevel();

    public long getDisposalTime();

    public void setNumberOfBuffers(int numberOfBuffers);

    public void setInitialCapacity(int initialCapacity);

    public void setRamMegaBytes(int ramMegaBytes);

    public void setConcurrencyLevel(int concurrencyLevel);

    public void setDisposalTime(long disposalTime);

}