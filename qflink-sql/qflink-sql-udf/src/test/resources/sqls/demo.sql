
CREATE TEMPORARY FUNCTION IF NOT EXISTS time_parse AS 'cn.syntomic.qflink.sql.udf.scalar.TimeParse' LANGUAGE JAVA;


SELECT
    time_parse('2023-04-09 14:00:00', 'yyyy-MM-dd HH:mm:ss', 'yyyy-MM-dd HH:mm:ss.SSS')
UNION ALL
SELECT
    time_parse('2023-04-09 14:00:00.234', 'yyyy-MM-dd HH:mm:ss', 'yyyy-MM-dd HH:mm:ss.SSS');
