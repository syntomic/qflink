-- The alerts the rule engine writes, the console copies each one it reads from the alert channel.
CREATE TABLE IF NOT EXISTS alerts (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    rule_id     INT     COMMENT '规则 id',
    alert_time  VARCHAR COMMENT '告警时间，作业写出的 yyyy-MM-dd HH:mm:ss',
    alert_keys  VARCHAR COMMENT '分组键，json',
    metrics     VARCHAR COMMENT '指标，json',
    received_at VARCHAR COMMENT '控制台收到它的时间，ISO-8601'
);
