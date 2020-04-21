CREATE TABLE t_ref_source
(
  id SERIAL PRIMARY KEY,
  code VARCHAR UNIQUE,
  name VARCHAR,
  url VARCHAR
);
INSERT INTO t_ref_source(code, name, url)
VALUES ('vk', 'ВКонтакте', 'https://m.vk.com/sber.sluh'),
       ('bankiru', 'Банки.Ру', 'https://www.banki.ru/services/official/bank/?ID=322');


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
