CREATE TABLE t_ref_source
(
  id SERIAL PRIMARY KEY,
  code VARCHAR UNIQUE,
  name VARCHAR,
  url VARCHAR
);
INSERT INTO t_ref_source(code, name, url)
VALUES ('vk', 'ВКонтакте', 'https://m.vk.com/sber.sluh'),
       ('bankiru', 'Банки.Ру - ', 'https://www.banki.ru/services/official/bank/?ID=322'),
       ('bankiru_clients', 'Банки.Ру - Народный рейтинг', 'https://www.banki.ru/services/responses/bank/sberbank/'),
       ('pikabu', 'Пикабу', 'https://pikabu.ru/tag/Сбербанк/hot');


CREATE TABLE t_ref_type
(
  id SERIAL PRIMARY KEY,
  code VARCHAR UNIQUE,
  name VARCHAR
);
INSERT INTO t_ref_type(code, name)
VALUES ('post', 'Пост'),
       ('comment', 'Комментарий');


CREATE TABLE t_ref_user
(
  id BIGSERIAL PRIMARY KEY,
  login VARCHAR,
  external_uid INTEGER,
  ref_source_id INTEGER REFERENCES t_ref_source(id),
  UNIQUE (login, external_uid, ref_source_id)
);


CREATE TABLE t_post_info
(
  id BIGSERIAL PRIMARY KEY,
  parent_id BIGINT,
  external_post_id INTEGER,
  ref_source_id INTEGER REFERENCES t_ref_source(id),
  ref_type_id INTEGER REFERENCES t_ref_type(id),
  ref_user_id BIGINT REFERENCES t_ref_user(id),
  content JSONB,
  datetime TIMESTAMP,
  UNIQUE (external_post_id, ref_source_id)
);



-- Запросы

SELECT *
  FROM (SELECT tpi.id
             , external_post_id
             , content ->> 'title' as title
             , regexp_replace(content ->> 'msg', '(\d){4}((\s)|[-\.:])*(\d){4}((\s)|[-\.:])*(\d){4}((\s)|[-\.:])*(\d){4}', '') as message
             , content ->> 'bank_answer' as bank_answer
             , content ->> 'rating' as rating
             , tru.login
             , datetime
          FROM t_post_info tpi
         INNER JOIN t_ref_user tru
               ON tpi.ref_user_id = tru.id
         WHERE tpi.ref_source_id = 3
               AND tpi.ref_type_id = 1
         ORDER BY datetime DESC) a;


WITH RECURSIVE grouped_posts as (
    SELECT 1 as level
         , external_post_id as group_base_id
         , tpi.*
      FROM t_post_info tpi
     WHERE parent_id IS NULL
     UNION ALL
    SELECT pp.level + 1 as level
         , pp.group_base_id
         , tpi.*
      FROM t_post_info tpi
     INNER JOIN grouped_posts pp
           ON tpi.parent_id = pp.id
           AND coalesce(tpi.external_post_id, pp.external_post_id) = pp.external_post_id
)
SELECT tbl.id
     , tbl.url
     , tbl.title
--      , tbl.rating
     , CASE WHEN tbl.ref_type_id = 1 THEN tbl.message END as post_message
--      , tbl.bank_answer
--      , CASE WHEN tbl.ref_type_id = 2 THEN tbl.message END as comment_message
     , tbl.login
     , tbl.user_url
     , tbl.datetime
  FROM (SELECT gp.id
             , gp.content ->> 'post_url' as url
             , gp.content ->> 'title' as title
             , regexp_replace(gp.content ->> 'msg', '(\d){4}((\s)|[-\.:])*(\d){4}((\s)|[-\.:])*(\d){4}((\s)|[-\.:])*(\d){4}', '') as message
             , gp.content ->> 'bank_answer' as bank_answer
             , gp.content ->> 'rating' as rating
             , tru.login
             , 'https://pikabu.ru/@' || tru.login as user_url
             , gp.datetime
             , gp.ref_type_id
          FROM grouped_posts gp
         INNER JOIN t_ref_user tru
               ON gp.ref_user_id = tru.id
         WHERE gp.ref_source_id = 4
         ORDER BY gp.group_base_id, gp.level, gp.datetime) tbl
 WHERE     length(tbl.message) > 1
       AND tbl.ref_type_id = 1;
