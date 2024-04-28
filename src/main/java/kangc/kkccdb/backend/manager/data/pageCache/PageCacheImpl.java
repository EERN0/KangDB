package kangc.kkccdb.backend.manager.data.pageCache;

import kangc.kkccdb.backend.common.AbstractCacheLru;
import kangc.kkccdb.backend.manager.data.page.Page;
import kangc.kkccdb.backend.manager.data.page.PageImpl;
import kangc.kkccdb.utils.Panic;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PageCacheImpl extends AbstractCacheLru<Page> implements PageCache {

    private static final int MEM_MIN_LIM = 10;

    // 对文件随机访问
    private final RandomAccessFile file;

    // 文件通道，与file关联输入输出操作
    private final FileChannel fileChannel;
    private final Lock fileLock;

    // 线程安全的页码计数器，记录当前打开的数据库文件有多少页（数据库文件打开就会计算，新增页面时自增）
    private final AtomicInteger pageNumbers;

    PageCacheImpl(RandomAccessFile file, FileChannel fileChannel, int maxResource) {
        super(maxResource);
        if (maxResource < MEM_MIN_LIM) {
            Panic.panic(new RuntimeException("Memory too small!"));
        }
        long length = 0;
        try {
            length = file.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        this.file = file;
        this.fileChannel = fileChannel;
        this.fileLock = new ReentrantLock();
        // 计算页码
        this.pageNumbers = new AtomicInteger((int) length / PAGE_SIZE);
    }

    /**
     * 创建一个新页面，并持久化
     *
     * @param initData 页面初始化数据
     * @return 页号
     */
    public int newPage(byte[] initData) {
        int pageNum = pageNumbers.incrementAndGet();
        Page pg = new PageImpl(pageNum, initData, null);
        // 新页立即写回
        flush(pg);
        return pageNum;
    }

    /**
     * 根据页号获取页，若页面不在缓存，通过getForCache()加载文件到缓存
     */
    public Page getPage(int pageNum) throws Exception {
        return get(pageNum);
    }

    /**
     * 根据页号（key），从文件获取页面数据，包裹成Page返回
     */
    @Override
    protected Page getForCache(long key) throws Exception {
        // 根据页号计算在文件中的偏移量
        int pageNum = (int) key;
        long offset = PageCacheImpl.pageOffset(pageNum);

        ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);
        fileLock.lock();
        try {
            fileChannel.position(offset);
            fileChannel.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        fileLock.unlock();
        return new PageImpl(pageNum, buf.array(), this);
    }

    /**
     * 页面从缓存删去之前调用，持久化脏页到文件
     */
    @Override
    protected void releaseForCache(Page pg) {
        if (pg.isDirty()) {
            flush(pg);
            pg.setDirty(false);
        }
    }

    /**
     * 从缓存中释放指定的页面
     */
    public void release(Page page) {
        release(page.getPageNumber());
    }

    public void flushPage(Page pg) {
        flush(pg);
    }

    /**
     * 把页面数据写入到文件，并强制同步到磁盘
     */
    private void flush(Page pg) {
        // 根据页号计算当前页在文件中的偏移量
        int pageNum = pg.getPageNumber();
        long offset = pageOffset(pageNum);

        fileLock.lock();
        try {
            // 将页面的字节数组包装成ByteBuffer，避免复制
            ByteBuffer buf = ByteBuffer.wrap(pg.getData());
            // 定位页面位置
            fileChannel.position(offset);
            fileChannel.write(buf);
            // 强制刷盘，保证写入数据不丢失
            fileChannel.force(true);
        } catch (IOException e) {
            Panic.panic(e);
        } finally {
            fileLock.unlock();
        }
    }

    /**
     * 截断文件，使其仅包含页号 ≤ maxPageNum的页面，更新页面计数器pageNumbers的值
     */
    public void truncateByPageNum(int maxPageNum) {
        long size = pageOffset(maxPageNum + 1);
        try {
            file.setLength(size);
        } catch (IOException e) {
            Panic.panic(e);
        }
        pageNumbers.set(maxPageNum);
    }

    /**
     * 关闭缓存、文件通道和文件
     */
    @Override
    public void close() {
        super.close();
        try {
            fileChannel.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    /**
     * 当前已用最大页号
     */
    public int getPageNumber() {
        return pageNumbers.intValue();
    }

    /**
     * 根据页号计算在文件中的偏移量
     */
    private static long pageOffset(int pageNum) {
        // 页号从 1 开始，一页8k
        return (long) (pageNum - 1) * PAGE_SIZE;
    }
}
