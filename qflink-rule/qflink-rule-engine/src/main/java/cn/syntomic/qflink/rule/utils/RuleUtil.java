package cn.syntomic.qflink.rule.utils;

import org.apache.flink.api.common.state.BroadcastState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.syntomic.qflink.rule.entity.Rule;

public class RuleUtil {

    private static final Logger LOG = LoggerFactory.getLogger(RuleUtil.class);

    /**
     * Update rule
     *
     * @param rule
     * @param broadcastState
     * @throws Exception
     */
    public static void updateRule(Rule rule, BroadcastState<Integer, Rule> broadcastState)
            throws Exception {

        LOG.info(
                "Rule Engine: Rule Operation with state {} and id {}",
                rule.getRuleState(),
                rule.getRuleId());

        switch (rule.getRuleState()) {
            case ACTIVE:
            case UPDATE:
                broadcastState.put(rule.getRuleId(), rule);
                break;
            case REMOVE:
                broadcastState.remove(rule.getRuleId());
                break;
            default:
                throw new IllegalArgumentException(rule.toString());
        }
    }

    private RuleUtil() {}
}
