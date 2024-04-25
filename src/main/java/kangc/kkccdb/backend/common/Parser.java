package kangc.kkccdb.backend.common;

import java.nio.ByteBuffer;

public class Parser {

    /**
     * 把缓冲区的前8个字节转成long
     */
    public static long parseLong(byte[] buf) {
        ByteBuffer buffer = ByteBuffer.wrap(buf, 0, 8);
        return buffer.getLong();
    }

    public static byte[] long2Byte(long value) {
        return ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(value).array();
    }
}
