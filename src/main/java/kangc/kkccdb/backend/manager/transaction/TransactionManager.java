package kangc.kkccdb.backend.manager.transaction;

import kangc.kkccdb.backend.common.Parser;
import kangc.kkccdb.utils.Panic;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 事务管理模块
 */
public class TransactionManager {

    // 事务ID文件头的长度，从文件的前8字节读出事务id
    private static final int TRXID_HEADER_LENGTH = 8;

    // 事务字段，这里只用1B来标识状态，跟在header后面
    private static final int TRXID_FIELD_SIZE = 1;

    // 事务状态，活跃、已提交、中止（已回滚）
    private static final byte TRAN_ACTIVE = 0;
    private static final byte TRAN_COMMITTED = 1;
    private static final byte TRAN_ABORTED = 2;

    // 超级事务，该事务永远为committed已提交状态
    public static final long SUPER_TRXID = 0;

    // 事务ID文件的 File 实例
    private final File file;

    // fileChannel用来操作file实例
    private final FileChannel fileChannel;

    // 当前分配到的最大事务ID
    private long trxIdCounter;
    private final Lock counterLock;

    private TransactionManager(File file, FileChannel fileChannel) {
        this.file = file;
        this.fileChannel = fileChannel;
        counterLock = new ReentrantLock();
        checkXIDCounter();
    }

    /**
     * 创建一个新的事务管理器，尝试创建一个新的事务ID文件
     */
    public static TransactionManager create(String path) {
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
        try {
            RandomAccessFile raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        // 写空trxId文件头
        ByteBuffer buf = ByteBuffer.wrap(new byte[TRXID_HEADER_LENGTH]);
        try {
            fc.position(0);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        return new TransactionManager(f, fc);
    }

    /**
     * 打开现有的事务ID文件，并创建事务管理器
     */
    public static TransactionManager open(String path) {
        File f = new File(path);
        if (!f.exists()) {
            Panic.panic(new RuntimeException("文件不存在!"));
        }
        if (!f.canRead() || !f.canWrite()) {
            Panic.panic(new RuntimeException("无权限读写文件"));
        }

        FileChannel fc = null;
        try {
            RandomAccessFile raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        return new TransactionManager(f, fc);
    }


    /**
     * 校验trxId事务ID文件，确保是合法的
     * 读取XID_FILE_HEADER中的trxidCounter，根据它计算文件的理论长度，对比实际长度
     */
    private void checkXIDCounter() {
        long fileLen = file.length();

        if (fileLen == 0) {
            Panic.panic(new RuntimeException("文件长度错误!"));
        }
        if (fileLen < TRXID_HEADER_LENGTH) {
            Panic.panic(new RuntimeException("不完整的事务ID文件!"));
        }
        // 创建 ByteBuffer 实例，分配与 TRXID_HEADER_LENGTH 大小相等的字节缓冲区
        ByteBuffer buf = ByteBuffer.allocate(TRXID_HEADER_LENGTH);
        try {
            // 读取文件的位置，0即文件开头
            fileChannel.position(0);
            // 从文件当前位置0，读取TRXID_HEADER_LENGTH 字节数据到 buf 中。缓冲区填充完后，buf指向数据末尾
            fileChannel.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        // 当前的事务id (事务文件中的前8字节数据转成long类型的事务id)
        this.trxIdCounter = Parser.parseLong(buf.array());
        long end = getTrxIdPosition(this.trxIdCounter + 1); // +1是因为事务状态也占一位
        if (end != fileLen) {
            Panic.panic(new RuntimeException("不完整的事务ID文件!"));
        }
    }

    /**
     * 根据事务ID (trxId) 计算在文件中的偏移量
     */
    private long getTrxIdPosition(long trxId) {
        return TRXID_HEADER_LENGTH + (trxId - 1) * TRXID_FIELD_SIZE;
    }

    /**
     * 更新当前事务的状态，写入到文件中
     *
     * @param trxId  事务id
     * @param status 状态
     */
    private void updateTrxId(long trxId, byte status) {
        // 事务id的在文件的偏移量
        long offset = getTrxIdPosition(trxId);
        // 事务的状态字段占1B
        byte[] tmp = new byte[TRXID_FIELD_SIZE];
        tmp[0] = status;
        ByteBuffer buf = ByteBuffer.wrap(tmp);
        try {
            fileChannel.position(offset);
            fileChannel.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        try {
            fileChannel.force(true);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    /**
     * 递增，为新事务分配新事务ID
     * 写入事务文件，持久化变更header，不用修改事务状态
     */
    private void incrTrxIdCounter() {
        trxIdCounter++;
        ByteBuffer buf = ByteBuffer.wrap(Parser.long2Byte(trxIdCounter));
        try {
            fileChannel.position(0);
            fileChannel.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        try {
            fileChannel.force(true);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    /**
     * 启动一个新事务，返回事务id（上一个事务id+1）
     */
    public long begin() {
        counterLock.lock();
        try {
            // 获取下一个事务id
            long trxId = trxIdCounter + 1;
            // 设置新事务状态为active，更新事务文件
            updateTrxId(trxId, TRAN_ACTIVE);
            // 更新当前事务id (内存中的trxIdCounter+1)
            incrTrxIdCounter();
            return trxId;
        } finally {
            counterLock.unlock();
        }
    }

    /**
     * 提交事务
     */
    public void commit(long trxId) {
        updateTrxId(trxId, TRAN_COMMITTED);
    }

    /**
     * 事务回滚
     */
    public void rollback(long trxId) {
        updateTrxId(trxId, TRAN_ABORTED);
    }

    /**
     * 检查当前事务是否处于status状态
     *
     * @param trxId  事务id
     * @param status 状态(active、committed、aborted)
     */
    private boolean checkTrxId(long trxId, byte status) {
        // 获取trxId事务的状态在文件中的偏移量
        long offset = getTrxIdPosition(trxId);
        ByteBuffer buf = ByteBuffer.wrap(new byte[TRXID_FIELD_SIZE]);
        try {
            fileChannel.position(offset);
            fileChannel.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        return buf.array()[0] == status;
    }

    /**
     * 检查事务状态，是否活跃
     */
    public boolean isActive(long trxId) {
        // super trx永远处于committed状态
        if (trxId == SUPER_TRXID) return false;
        return checkTrxId(trxId, TRAN_ACTIVE);
    }

    /**
     * 检查事务状态，是否提交
     */
    public boolean isCommitted(long trxId) {
        // super trx永远处于committed状态
        if (trxId == SUPER_TRXID) return true;
        return checkTrxId(trxId, TRAN_COMMITTED);
    }

    /**
     * 检查事务状态，是否回滚
     */
    public boolean isAborted(long trxId) {
        // super trx永远处于committed状态
        if (trxId == SUPER_TRXID) return false;
        return checkTrxId(trxId, TRAN_ABORTED);
    }

    public void close() {
        try {
            fileChannel.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

}
