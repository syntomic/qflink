SELECT title, cover, area, AVG(score) AS score, SUM(play_cnt) AS play_cnt
FROM movie
WHERE area = {{$area}}
GROUP BY title, cover, area
ORDER BY {{$order}}
LIMIT {{$limit}} OFFSET {{$offset}}
