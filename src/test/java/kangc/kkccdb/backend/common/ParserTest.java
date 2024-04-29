package kangc.kkccdb.backend.common;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class ParserTest {

    @Test
    public void testParseLongAndLong2Byte() {
        // 测试一个典型的长整数值
        long originalValue = 1234567890L;
        // 1、使用 long2Byte 转换长整数为字节数组
        byte[] bytes = Parser.long2Byte(originalValue);
        // 打印转换为字节数组后的各个字节的值
        System.out.print("Bytes: [");
        for (int i = 0; i < bytes.length; i++) {
            // 转换为无符号整数打印
            int unsignedByte = bytes[i] & 0xFF;
            System.out.print(unsignedByte);
            if (i < bytes.length - 1) {
                System.out.print(", ");
            }
        }
        System.out.println("]");

        // 2、使用 parseLong 将字节数组转换回长整数
        long parsedValue = Parser.parseLong(bytes);
        System.out.println("parsedValue = " + parsedValue);

        // 断言原始长整数值和解析后的长整数值相等
        assertEquals("The parsed long value should be equal to the original value.", originalValue, parsedValue);

        // 再次使用 long2Byte 转换解析后的长整数为字节数组
        byte[] bytesAfterParse = Parser.long2Byte(parsedValue);

        // 断言原始字节数组和解析后再次转换得到的字节数组相等
        assertArrayEquals("The byte array after parsing and converting back should be equal to the original byte array.", bytes, bytesAfterParse);
    }
}
