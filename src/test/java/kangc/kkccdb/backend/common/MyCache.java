package kangc.kkccdb.backend.common;

public class MyCache extends AbstractCacheLru<Long> {

    public MyCache() {
        super(50);
    }

    @Override
    protected Long getForCache(long key) throws Exception {
        return key;
    }

    @Override
    protected void releaseForCache(Long obj) {
    }
}
