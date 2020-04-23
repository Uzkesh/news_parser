# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

from news_parser.common.db_manager import DB, DBParamsDTO
from news_parser.common.types import DBPostTypesDTO
from news_parser.common.regexp_template import RegExp
from news_parser.items import PostBankiru, CommentBankiru
from typing import Optional, Union
import json


class PipelineException(Exception):
    pass


class NewsParserPipeline(object):
    def __init__(self, params: DBParamsDTO):
        self._params = params
        self.db = None
        self._current_post_id = None
        self._source_id = None
        self._post_types: Optional[DBPostTypesDTO] = None

    @classmethod
    def from_crawler(cls, crawler):
        # TODO: Перенести подлючение к БД в middleware, а не для каждого паука отдельно !!!
        db_params = crawler.settings.getdict("DB_PARAMS")
        return cls(DBParamsDTO(
            host=db_params["host"],
            port=db_params["port"],
            name=db_params["name"],
            user=db_params["user"],
            password=db_params["password"]
        ))

    def open_spider(self, spider):
        self.db = DB(self._params)
        self._source_id = int(self._get_source_id(spider.name))
        self._post_types = self._get_types_id()
        # TODO: Получать последний сохраненный ID поста
        # TODO: Добавить параметризованный запуск паука из командной строки:
        # TODO: Только новые посты; Добавление новых и обновление существующих до определенной даты; Добавление новых до определенной даты.

        # spider.last_post_id("0")
        # spider.last_post_id(self._get_last_post_id())

    def close_spider(self, spider):
        self.db.disconnect()
        self.db = None
        self._current_post_id = None
        self._source_id = None
        self._post_types = None

    def process_item(self, item, spider):
        if len(item.keys()) > 0:
            self._current_post_id = item.get("post_id", None)

            if spider.name == "vk":
                pass
                # self.db.cur.execute(
                #     RegExp.space.sub(" ", """
                #         INSERT INTO t_vk_posts(part, ntime, msg)
                #         VALUES (:p_part, :p_ntime, :p_msg)
                #     """),
                #     {
                #         "p_part": item["part"],
                #         "p_ntime": item["ntime"],
                #         "p_msg": item["msg"]
                #     }
                # )

            elif spider.name == "bankiru" or spider.name == "bankiru_clients" or spider.name == "pikabu":
                # try:
                    self._save_post_info(
                        parent_id=None,
                        post_type=self._post_types.post,
                        item=item,
                        comments=item.get("comments", list())
                    )
                # except PipelineException as e:
                #     print(str(e))

        return item

    def _save_post_info(self, parent_id: Optional[int], post_type: int, item, comments: list):
        _etag = str(NewsParserPipeline._save_post_info.__qualname__)

        # Ищем пользователя
        user_id = self._get_user_id(
            uid=item["author_uid"],
            login=item["author_login"]
        )

        # Создаем запись нового пользователя
        if user_id is None:
            user_id = self._add_new_user(
                uid=item["author_uid"],
                login=item["author_login"]
            )

        # Если не удалось найти и добавить пользователя (хз на уровне БД)
        if user_id is None:
            raise PipelineException(
                f"{_etag}.1:Не удалось сохранить информацию о пользователе # [{self._source_id=}, {self._current_post_id=}]: [{item['author_uid']=}, {item['author_login']}]"
            )

        record_post_id = self._add_post_info(
            parent_id=parent_id,
            user_id=int(user_id),
            post_type=post_type,
            item=item
        )
        self.db.commit()

        for i in comments:
            try:
                self._save_post_info(
                    parent_id=record_post_id,
                    post_type=self._post_types.comment,
                    item=i,
                    comments=i.get("comments", list())
                )
            except PipelineException as e:
                print(str(e))

    def _get_last_post_id(self):
        """
        Получить последний external_post_id текущего ресурса
        :return:
        """
        self.db.cur.execute(
            RegExp.space.sub(" ", """
                SELECT max(external_post_id) as last_post_id
                  FROM t_post_info
                 WHERE     ref_source_id = %(p_source_id)s
                       AND ref_type_id = %(p_type_id)s
            """),
            {
                "p_source_id": self._source_id,
                "p_type_id": self._post_types.post
            }
        )
        return self.db.cur.fetchone()[0] or "0"

    def _get_source_id(self, source_code: str):
        _etag = str(NewsParserPipeline._get_source_id.__qualname__)

        self.db.cur.execute(
            RegExp.space.sub(" ", """
                SELECT id
                  FROM t_ref_source
                 WHERE code = %(p_code)s
            """),
            {"p_code": source_code}
        )
        res = (self.db.cur.fetchone() or [None])[0]

        if res is None:
            raise PipelineException(f"{_etag}.1: ID ресурса не найден # [{source_code}]")

        return res

    def _get_types_id(self):
        post, comment = None, None

        self.db.cur.execute(RegExp.space.sub(" ", """
            SELECT id, code
              FROM t_ref_type
        """))

        for record in self.db.cur:
            if record[1] == "post":
                post = record[0]
            elif record[1] == "comment":
                comment = record[0]

        return DBPostTypesDTO(post=int(post), comment=int(comment))

    def _get_user_id(self, uid: int, login: str):
        self.db.cur.execute(
            RegExp.space.sub(" ", """
                SELECT id
                  FROM t_ref_user
                 WHERE     login = %(p_login)s
                       AND ref_source_id = %(p_source_id)s
                       AND (external_uid = %(p_uid)s
                            OR external_uid IS NULL)
                 ORDER BY coalesce(external_uid, -1) DESC
            """),
            {
                "p_login": login,
                "p_uid": uid,
                "p_source_id": self._source_id
            }
        )
        return (self.db.cur.fetchone() or [None])[0]

    def _add_new_user(self, uid: int, login: str):
        self.db.cur.execute(
            RegExp.space.sub(" ", """
                INSERT INTO t_ref_user(login, external_uid, ref_source_id) 
                VALUES (%(p_login)s, %(p_uid)s, %(p_source_id)s)
                RETURNING id
            """),
            {
                "p_login": login,
                "p_uid": uid,
                "p_source_id": self._source_id
            }
        )
        return self.db.cur.fetchone()[0]

    def _add_post_info(self, parent_id: Optional[int], user_id: int, post_type: int, item: Union[PostBankiru, CommentBankiru]):
        _etag = str(NewsParserPipeline._add_post_info.__qualname__)

        # TODO: Добавить тег - URL-поста
        # Формирование json для записи в БД в колонку content в зависимости от типа сообщения и ресурса
        type_item = type(item)
        if type_item is PostBankiru:
            content = json.dumps({
                "post_url": item.get("post_url", None),
                "title": item["title"],
                "rating": item.get("rating", None),
                "msg": item["msg"],
                "bank_answer": item["bank_answer"]
            })
        elif type_item is CommentBankiru:
            content = json.dumps({
                "msg": item["msg"]
            })
        else:
            raise PipelineException(f"{_etag}.1: Неизвестный формат данных # [{type_item=}]")

        self.db.cur.execute(
            RegExp.space.sub(" ", """
                INSERT INTO t_post_info(parent_id, external_post_id, ref_source_id, ref_type_id, ref_user_id, content, datetime)
                VALUES (%(p_parent_id)s, %(p_post_id)s, %(p_src_id)s, %(p_type_id)s, %(p_user_id)s, %(p_content)s, %(p_datetime)s)
                RETURNING id
            """),
            {
                "p_parent_id": parent_id,
                "p_post_id": item.get("post_id", None),
                "p_src_id": self._source_id,
                "p_type_id": post_type,
                "p_user_id": user_id,
                "p_content": content,
                "p_datetime": item.get("datetime", None)
            }
        )
        return self.db.cur.fetchone()[0]
