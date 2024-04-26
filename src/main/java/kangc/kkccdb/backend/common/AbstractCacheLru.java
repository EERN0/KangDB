package kangc.kkccdb.backend.common;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 实现一个LRU缓存淘汰策略
 */
public abstract class AbstractCacheLru<T> {

    // 链表，维护资源的访问顺序。最右端表示最不常被访问的数据
    private final LinkedList<Long> cacheKeysList;   // 最新使用的资源放到头部（头插法）

    // 缓存的数据
    private final HashMap<Long, T> cache;

    // 当前资源key，是否有线程在操作
    private final ConcurrentHashMap<Long, Boolean> getting;

    // 最大容量
    private final int maxResource;
    private final Lock lock;

    public AbstractCacheLru(int maxResource) {
        cacheKeysList = new LinkedList<>();
        cache = new HashMap<>();
        getting = new ConcurrentHashMap<>();
        this.maxResource = maxResource;
        lock = new ReentrantLock();
    }

    public T get(long key) throws Exception {
        while (true) {
            lock.lock();
            // 请求的资源key，其它线程在操作
            if (getting.containsKey(key)) {
                lock.unlock();
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    continue;
                }
                continue;
            }

            // key在cache中存在，直接返回对应的value
            if (cache.containsKey(key)) {
                T obj = cache.get(key);
                // 先删，再从头部添加key
                cacheKeysList.remove(key);
                cacheKeysList.addFirst(key);
                lock.unlock();
                return obj;
            }

            // 资源key不在缓存中，需要从其它地方获取（磁盘等）
            getting.put(key, true);     // 当前线程正在操作这个资源
            lock.unlock();
            break;
        }

        T obj = null;
        try {
            // 从外界获取key对应的数据
            obj = getForCache(key);
        } catch (Exception e) {
            lock.lock();
            getting.remove(key);
            lock.lock();
            throw e;
        }

        lock.lock();
        getting.remove(key);
        if (cache.size() == maxResource) {
            // 缓存满，淘汰最不常使用的key
            release(cacheKeysList.getLast());
        }
        // 把从外界获取的key，插入缓存
        cache.put(key, obj);
        cacheKeysList.addFirst(key);
        lock.unlock();

        return obj;
    }

    /**
     * 淘汰一个最不常用的key
     */
    public void release(long key) {
        lock.lock();
        try {
            T obj = cache.get(key);
            if (obj == null) return;
            releaseForCache(obj);
            // 缓存map、链表中，同时删掉key
            cache.remove(key);
            cacheKeysList.remove(key);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 关闭缓存，写回所有资源
     */
    public void close() {
        lock.lock();
        try {
            Set<Long> keys = cache.keySet();
            for (long key : keys) {
                release(key);
                cache.remove(key);
                cacheKeysList.remove(key);
            }
        } finally {
            lock.unlock();
        }
    }


    /**
     * 当资源key不在缓存中（内存），由实现类重写资源的获取策略（可以从磁盘拿到对应数据）
     */
    protected abstract T getForCache(long key) throws Exception;

    /**
     * key从内存被淘汰时，写回策略
     */
    protected abstract void releaseForCache(T obj);
}
