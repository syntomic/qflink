package cn.syntomic.qflink.jobs;

import cn.syntomic.qflink.common.jobs.JobEntrance;

public class FlinkJob {

    /**
     * The flink job unified entrance
     *
     * @param args
     */
    public static void main(String[] args) {
        JobEntrance.run(args, "cn.syntomic.qflink.jobs.impls.WordCountJob");
    }
}
