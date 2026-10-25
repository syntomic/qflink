SELECT rule_id, COUNT(*) AS alerts, MIN(alert_time) AS first_alert, MAX(alert_time) AS last_alert
FROM alerts
GROUP BY rule_id
ORDER BY {{$order}}
LIMIT {{$limit}}
