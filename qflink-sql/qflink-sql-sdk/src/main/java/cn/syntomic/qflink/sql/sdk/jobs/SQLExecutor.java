package cn.syntomic.qflink.sql.sdk.jobs;

import java.util.regex.Pattern;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.syntomic.qflink.sql.sdk.utils.SQLUtil;

public class SQLExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(SQLExecutor.class);

    private static final Pattern STATEMENT_SPLIT = Pattern.compile("( |$)", Pattern.MULTILINE);

    protected final StreamTableEnvironment tableEnv;
    protected final StreamStatementSet statementSet;
    protected final Configuration configuration;

    protected Boolean hasInsert = false;

    private SQLExecutor(StreamTableEnvironment tableEnv) {
        this.tableEnv = tableEnv;
        this.statementSet = tableEnv.createStatementSet();
        this.configuration = tableEnv.getConfig().getConfiguration();
    }

    /**
     * Execute sqls by table environment
     *
     * @param sqls
     */
    public void execute(String[] sqls) {
        execute(sqls, true);
    }

    /**
     * Execute sqls by table environment or not
     *
     * @param sqls
     * @param executeEnable
     */
    public void execute(String[] sqls, boolean executeEnable) {
        executePartial(sqls);
        if (executeEnable && hasInsert) {
            statementSet.execute();
        } else {
            statementSet.attachAsDataStream();
        }
    }

    /**
     * Execute multi sqls once
     *
     * @param sqls
     */
    private void executePartial(String[] sqls) {
        for (String sql : sqls) {
            if (StringUtils.isBlank(sql)) {
                continue;
            }

            sql = sql.trim();
            LOG.info(SQLUtil.maskSql(sql));

            String statement = STATEMENT_SPLIT.split(sql, 2)[0];
            switch (statement.toUpperCase()) {
                case "SET":
                    String[] setConfig = sql.replaceAll("SET +", "").split("=");
                    // TODO support execution.runtime-mode configuration
                    if (setConfig.length == 2) {
                        // not need quote
                        configuration.setString(
                                setConfig[0].replaceAll("['\"]", "").trim(),
                                setConfig[1].replaceAll("['\"]", "").trim());
                    } else {
                        LOG.warn("SET should specify key and value");
                    }
                    break;
                case "RESET":
                    LOG.warn("RESET Just need to delete the SET statement");
                    break;
                case "EXPLAIN":
                case "DESCRIBE":
                case "DESC":
                case "SHOW":
                    tableEnv.executeSql(sql).print();
                    break;
                case "SELECT":
                    LOG.warn(
                            "SELECT statement can only be used in develop environment, please do not use in produce environment");
                    tableEnv.executeSql(sql).print();
                    break;
                case "INSERT":
                    // use statement set to insert
                    hasInsert = true;
                    statementSet.addInsertSql(sql);
                    break;
                default:
                    tableEnv.executeSql(sql);
                    break;
            }
        }
    }

    /**
     * Create SQLExecutor
     *
     * @param tableEnv
     * @return
     */
    public static SQLExecutor of(StreamTableEnvironment tableEnv) {
        return new SQLExecutor(tableEnv);
    }
}
