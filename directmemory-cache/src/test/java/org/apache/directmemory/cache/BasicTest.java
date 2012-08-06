package org.apache.directmemory.cache;

import static org.junit.Assert.*;

import org.apache.directmemory.DirectMemory;
import org.junit.Test;

public class BasicTest {

	@Test
	public void test() 
	{
		CacheService<String, Long> cache = new DirectMemory<String, Long>()
	            .setNumberOfBuffers(10)
	            .setSize(1000)
	            .setInitialCapacity(100000)
	            .setConcurrencyLevel(4)
	            .newCacheService();

	        assertNull(cache.retrieve("a"));
	        assertNotNull(cache.put("a", 3L));
	        assertNotNull(cache.retrieve("a"));
	        assertEquals(3L, cache.retrieve("a").longValue());
	        assertNotNull(cache.put("a", 5L));
	        assertNotNull(cache.retrieve("a"));
	        assertEquals(5L, cache.retrieve("a").longValue());
	}

}
