package cn.syntomic.qflink.sql.sdk;

/**
 * Local launcher of a sql job, it is a {@code main} and not a junit test.
 *
 * <p>The job configuration is read from {@code src/test/resources/example.json}.
 */
public class EndToEndTest {
    public static void main(String[] args) throws Exception {
        FlinkSQL.main(args);
    }
}
