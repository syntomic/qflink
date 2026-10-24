package cn.syntomic.ffi;

/** Java 传一个日期给 Python，Python 告诉我们这天星期几。 */
public class FFIDemo {

    static {
        // 加载用 C 写的“桥”。加载时 JVM 会调用其中的 JNI_OnLoad，Python 解释器就在那里启动
        System.load(System.getProperty("user.dir") + "/" + System.mapLibraryName("ffidemo"));
    }

    /** native 方法：Java 这里只有声明，实现写在 C 里 */
    native int dayOfWeek(String date);

    public static void main(String[] args) {
        System.out.println(new FFIDemo().dayOfWeek(args.length > 0 ? args[0] : "1994-05-05"));
    }
}
