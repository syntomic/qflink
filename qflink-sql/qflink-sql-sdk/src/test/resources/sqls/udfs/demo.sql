CREATE VIEW Names(name) AS VALUES ('Bob'), ('Alice'), ('Bob');

CREATE TEMPORARY FUNCTION IF NOT EXISTS Greeting AS 'cn.syntomic.qflink.sql.udf.ptfs.Greeting' LANGUAGE JAVA;

SELECT * FROM Greeting(TABLE Names);