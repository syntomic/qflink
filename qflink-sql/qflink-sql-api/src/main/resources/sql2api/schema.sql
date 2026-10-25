-- The movie table of the article 《SQL Is All Your Need: SQL2API》, a few made-up rows.
CREATE TABLE movie (
    title     VARCHAR COMMENT '电影名称',
    cover     VARCHAR COMMENT '电影封面',
    area      VARCHAR COMMENT '电影地区',
    style     VARCHAR COMMENT '电影风格',
    score     DOUBLE  COMMENT '电影评分',
    play_cnt  BIGINT  COMMENT '电影播放次数'
);

INSERT INTO movie VALUES
    ('坏蛋联盟', 'https://example.com/cover/1.png', '美国', '喜剧', 9.6, 110000000),
    ('星际穿越', 'https://example.com/cover/2.png', '美国', '科幻', 9.4, 86000000),
    ('盗梦空间', 'https://example.com/cover/3.png', '美国', '科幻', 9.3, 72000000),
    ('流浪地球', 'https://example.com/cover/4.png', '中国大陆', '科幻', 8.9, 95000000),
    ('你好，李焕英', 'https://example.com/cover/5.png', '中国大陆', '喜剧', 8.1, 64000000),
    ('千与千寻', 'https://example.com/cover/6.png', '日本', '动画', 9.4, 88000000);
