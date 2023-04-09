package cn.syntomic.qflink.python.impls;

import cn.syntomic.qflink.common.jobs.AbstractJob;
import cn.syntomic.qflink.sql.sdk.jobs.SQLJob;

public class FlinkSQLWithPythonUDF extends AbstractJob {

    @Override
    public void run() throws Exception {
        SQLJob.of(conf, env).run();
    }
}
