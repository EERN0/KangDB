package kangc.kkccdb.backend.manager.data.pageCache;

import kangc.kkccdb.backend.manager.data.page.Page;
import kangc.kkccdb.utils.Panic;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

public interface PageCache {

    // 页面大小8KB, 2^13
    public static final int PAGE_SIZE = 1 << 13;

    /**
     * 创建新页，返回页号
     */
    int newPage(byte[] initData);

    /**
     * 通过页号获取页
     */
    Page getPage(int pageNo) throws Exception;

    void close();

    void release(Page page);

    /**
     * 保留页号小于或等于maxPageNum的页面
     */
    void truncateByPageNum(int maxPageNum);

    /**
     * 返回当前页面缓存中的页面数量
     */
    int getPageNumber();

    /**
     * 页刷盘
     */
    void flushPage(Page pg);

    public static PageCacheImpl create(String path, long memory) {
        File f = new File(path);
        try {
            if (!f.createNewFile()) {
                Panic.panic(new RuntimeException("文件已存在!"));
            }
        } catch (Exception e) {
            Panic.panic(e);
        }
        if (!f.canRead() || !f.canWrite()) {
            Panic.panic(new RuntimeException("无权限读写文件"));
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        return new PageCacheImpl(raf, fc, (int) memory / PAGE_SIZE);
    }

    public static PageCacheImpl open(String path, long memory) {
        File f = new File(path);
        if (!f.exists()) {
            Panic.panic(new RuntimeException("文件不能再!"));
        }
        if (!f.canRead() || !f.canWrite()) {
            Panic.panic(new RuntimeException("无权限读写文件"));
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        return new PageCacheImpl(raf, fc, (int) memory / PAGE_SIZE);
    }
}
