package kangc.kkccdb.utils;

/**
 * 打印出异常信息，强制停机
 */
public class Panic {
    public static void panic(Exception e) {
        e.printStackTrace();
        System.exit(1);
    }
}
