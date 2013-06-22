package net.sf.ehcache;

import net.sf.ehcache.pool.Pool;
import net.sf.ehcache.pool.PoolableStore;
import net.sf.ehcache.store.Store;
import net.sf.ehcache.store.offheap.OffHeapStore;
import net.sf.ehcache.transaction.SoftLockFactory;
import net.sf.ehcache.transaction.SoftLockManager;
import net.sf.ehcache.transaction.TransactionIDFactory;
import net.sf.ehcache.util.UpdateChecker;
import net.sf.ehcache.writer.writebehind.WriteBehind;

/**
 * Class name is hardcoded in ehcache so we need this one!!
 * @author Olivier Lamy
 * @since 0.2
 */
public class EnterpriseFeaturesManager
    implements FeaturesManager
{

    private  CacheManager cacheManager;

    public EnterpriseFeaturesManager( CacheManager cacheManager )
    {
        this.cacheManager = cacheManager;
    }

    @Override
    public WriteBehind createWriteBehind( Cache cache )
    {
        return null;
    }

    @Override
    public Store createStore( Cache cache, Pool<PoolableStore> onHeapPool, Pool<PoolableStore> onDiskPool )
    {
        return OffHeapStore.createOffHeapStore( cache );
    }

    @Override
    public TransactionIDFactory createTransactionIDFactory()
    {
        return null;
    }

    @Override
    public SoftLockManager createSoftLockManager( Ehcache cache, SoftLockFactory lockFactory )
    {
        return null;
    }

    @Override
    public void startup()
    {

    }

    @Override
    public void dispose()
    {

    }

    @Override
    public UpdateChecker createUpdateChecker()
    {
        return null;
    }
}
