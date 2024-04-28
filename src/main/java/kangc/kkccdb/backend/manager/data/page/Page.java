package kangc.kkccdb.backend.manager.data.page;

public interface Page {
    void lock();

    void unlock();

    /**
     * 释放缓存
     */
    void release();

    /**
     * 设置脏页
     */
    void setDirty(boolean dirty);

    boolean isDirty();

    int getPageNumber();

    byte[] getData();
}
