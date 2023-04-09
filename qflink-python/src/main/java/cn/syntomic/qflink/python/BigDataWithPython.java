package cn.syntomic.qflink.python;

import cn.syntomic.qflink.common.jobs.JobEntrance;

public class BigDataWithPython {
    public static void main(String[] args) {
        JobEntrance.run(args, "cn.syntomic.qflink.python.impls.FlinkSQLWithPythonUDF");
    }
}
