package kangc.kkccdb.backend.manager.data.page;

import kangc.kkccdb.backend.manager.data.pageCache.PageCache;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PageImpl implements Page {

    // 页号，从1开始
    private final int pageNumber;

    // 页实际存储的数据
    private final byte[] data;

    // 淘汰脏页，需要写回磁盘
    private boolean dirty;
    private final Lock lock;

    // 缓存页，通过Page实例快速释放缓存页
    private final PageCache pageCache;

    public PageImpl(int pageNumber, byte[] data, PageCache pageCache) {
        this.pageNumber = pageNumber;
        this.data = data;
        this.pageCache = pageCache;
        lock = new ReentrantLock();
    }

    public void lock() {
        lock.lock();
    }

    public void unlock() {
        lock.unlock();
    }

    public void release() {
        pageCache.release(this);
    }

    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    public boolean isDirty() {
        return dirty;
    }

    public int getPageNumber() {
        return pageNumber;
    }

    public byte[] getData() {
        return data;
    }
}