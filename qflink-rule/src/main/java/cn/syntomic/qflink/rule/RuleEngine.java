package cn.syntomic.qflink.rule;

import cn.syntomic.qflink.common.jobs.JobEntrance;

/** Rule-driven unified Flink job */
public class RuleEngine {

    public static void main(String[] args) {
        JobEntrance.run(args, "cn.syntomic.qflink.rule.RuleEngineJob");
    }
}
