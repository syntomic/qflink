package cn.syntomic.ffi;

import java.util.ArrayList;
import java.util.List;

/** Java 传一个日期给 Python，Python 告诉我们这天星期几。 */
public class FFIDemo {

    static {
        // 加载用 C 写的“桥”。加载时 JVM 会调用其中的 JNI_OnLoad，Python 解释器就在那里启动
        System.load(System.getProperty("user.dir") + "/" + System.mapLibraryName("ffidemo"));
    }

    /** native 方法：Java 这里只有声明，实现写在 C 里 */
    native int dayOfWeek(String date);

    /** 把 total 次调用平分给 threads 个 Java 线程，等它们都跑完。 */
    static void run(FFIDemo demo, int threads, int total) throws InterruptedException {
        List<Thread> workers = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            Thread worker =
                    new Thread(
                            () -> {
                                for (int n = 0; n < total / threads; n++) {
                                    demo.dayOfWeek("1994-05-05");
                                }
                            });
            workers.add(worker);
            worker.start();
        }
        for (Thread worker : workers) {
            worker.join();
        }
    }

    /** 同样的调用次数，换不同的线程数，看 GIL 让多线程变快还是变慢。 */
    static void bench(FFIDemo demo, int total) throws InterruptedException {
        // 预热一轮：让 JIT 编译、Python 导入的缓存都就位，不计入结果
        run(demo, 1, total);
        System.out.printf("%d 次调用%n", total);
        for (int threads : new int[] {1, 2, 4}) {
            long start = System.nanoTime();
            run(demo, threads, total);
            System.out.printf("%d 个线程：%.2f 秒%n", threads, (System.nanoTime() - start) / 1e9);
        }
    }

    public static void main(String[] args) throws InterruptedException {
        FFIDemo demo = new FFIDemo();
        if (args.length > 0 && args[0].equals("bench")) {
            bench(demo, args.length > 1 ? Integer.parseInt(args[1]) : 400_000);
        } else {
            System.out.println(demo.dayOfWeek(args.length > 0 ? args[0] : "1994-05-05"));
        }
    }
}
