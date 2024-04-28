package kangc.kkccdb.backend.common;

import org.junit.Test;

import java.util.Random;
import java.util.concurrent.CountDownLatch;

/**
 * LRU缓存测试
 */
public class CacheTest {

    private CountDownLatch latch;
    private MyCache cache;

    @Test
    public void testCache() {
        cache = new MyCache();
        latch = new CountDownLatch(1000);

        for (int i = 0; i < 1000; i++) {
            Runnable r = this::work;
            new Thread(r).start();
        }
        try {
            // 主线程等待所有子线程执行完
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void work() {
        for (int i = 0; i < 1000; i++) {
            long uid = new Random(System.nanoTime()).nextInt();
            long h;
            try {
                h = cache.get(uid);
            } catch (Exception e) {
                continue;
            }
            // 验证从缓存获取的uid等于
            assert h == uid;
        }
        latch.countDown();
    }
}
