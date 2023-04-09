package cn.syntomic.qflink.sql.sdk.jobs;

import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.utils.TypeConversions;

import cn.syntomic.qflink.common.utils.EnvUtil;

/** Connector use SQL but complex logical use DataStream */
public class IntegrateJob extends SQLJob {

    private final boolean executeEnable;

    public IntegrateJob(
            Configuration conf, StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {
        super(conf, env, tableEnv);

        this.executeEnable = false;
    }

    @Override
    public void run() throws Exception {
        String[] sqls = SQLFactory.of(conf).readSqls();
        SQLExecutor.of(tableEnv).execute(sqls, executeEnable);
    }

    /**
     * from table path to get table row type info
     *
     * @param tablePath
     * @return
     */
    @SuppressWarnings("deprecation")
    public RowTypeInfo getRowTypeInfo(String tablePath) {
        Table table = tableEnv.from(tablePath);
        return (RowTypeInfo)
                TypeConversions.fromDataTypeToLegacyInfo(
                        table.getResolvedSchema().toPhysicalRowDataType());
    }

    public static IntegrateJob of(Configuration conf) {
        StreamExecutionEnvironment env = EnvUtil.setStreamEnv(conf);
        return new IntegrateJob(conf, env, EnvUtil.setStreamTableEnv(env, conf));
    }

    public static IntegrateJob of(Configuration conf, StreamExecutionEnvironment env) {
        return new IntegrateJob(conf, env, EnvUtil.setStreamTableEnv(env, conf));
    }
}
