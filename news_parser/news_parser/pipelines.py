# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

from news_parser.common.db_manager import DB, DBParamsDTO
from news_parser.common.types import DBPostTypesDTO
from news_parser.common.regexp_template import RegExp
from news_parser.items import Post, Comment
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
        # print("!")
        db_params = crawler.settings.getdict("DB_PARAMS")
        return cls(DBParamsDTO(
            host=db_params["host"],
            port=db_params["port"],
            name=db_params["name"],
            user=db_params["user"],
            password=db_params["password"]
        ))

    def open_spider(self, spider):
        # TODO: Добавить параметризованный запуск паука из командной строки
        self.db = DB(self._params)
        self._source_id = int(self._get_source_id(spider.name))
        self._post_types = self._get_types_id()
        spider.set_last_post_id(self._get_last_post_id())

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
        account_id = self._get_account_id(
            uid=str(item["author_uid"]),
            login=str(item["author_login"])
        )

        # Создаем запись нового пользователя
        if account_id is None:
            account_id = self._add_new_account(
                uid=str(item["author_uid"]),
                login=str(item["author_login"])
            )

        # Если не удалось найти и добавить пользователя (хз на уровне БД)
        if account_id is None:
            raise PipelineException(
                f"{_etag}.1:Не удалось сохранить информацию о пользователе # [{self._source_id=}, {self._current_post_id=}]: [{item['author_uid']=}, {item['author_login']}]"
            )

        record_post_id = self._add_post_info(
            parent_id=parent_id,
            account_id=int(account_id),
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
                # TODO: Изменить обработку исключения
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
                 WHERE     ref_source = %(p_source_id)s
                       AND ref_type = %(p_type_id)s
            """),
            {
                "p_source_id": self._source_id,
                "p_type_id": self._post_types.post
            }
        )
        return self.db.cur.fetchone()[0]

    def _get_source_id(self, source_code: str):
        _etag = str(NewsParserPipeline._get_source_id.__qualname__)

        self.db.cur.execute(
            RegExp.space.sub(" ", """
                SELECT id
                  FROM ref_source
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
              FROM ref_type
             WHERE code IN ('post', 'comment')
        """))

        for record in self.db.cur:
            if record[1] == "post":
                post = record[0]
            elif record[1] == "comment":
                comment = record[0]

        return DBPostTypesDTO(post=int(post), comment=int(comment))

    def _get_account_id(self, uid: str, login: str):
        self.db.cur.execute(
            RegExp.space.sub(" ", """
                SELECT id
                  FROM ref_account
                 WHERE     login = %(p_login)s
                       AND ref_source = %(p_source_id)s
                       AND (external_uid = %(p_uid)s
                            OR external_uid IS NULL)
                 ORDER BY coalesce(external_uid, '') DESC
            """),
            {
                "p_login": login,
                "p_uid": uid,
                "p_source_id": self._source_id
            }
        )
        return (self.db.cur.fetchone() or [None])[0]

    def _add_new_account(self, uid: str, login: str):
        self.db.cur.execute(
            RegExp.space.sub(" ", """
                INSERT INTO ref_account(external_uid, login, ref_source) 
                VALUES (%(p_uid)s, %(p_login)s, %(p_source_id)s)
                RETURNING id
            """),
            {
                "p_uid": uid,
                "p_login": login,
                "p_source_id": self._source_id
            }
        )
        return self.db.cur.fetchone()[0]

    def _add_post_info(self, parent_id: Optional[int], account_id: int, post_type: int, item: Union[Post, Comment]):
        _etag = str(NewsParserPipeline._add_post_info.__qualname__)

        # TODO: Добавить тег - URL-поста
        # Формирование json для записи в БД в колонку content в зависимости от типа сообщения и ресурса
        type_item = type(item)
        if type_item is Post:
            content = json.dumps({
                "post_url": item.get("post_url", None),
                "title": item["title"],
                "rating": item.get("rating", None),
                "msg": item["msg"],
                "bank_answer": item["bank_answer"]
            })
        elif type_item is Comment:
            content = json.dumps({
                "msg": item["msg"]
            })
        else:
            raise PipelineException(f"{_etag}.1: Неизвестный формат данных # [{type_item=}]")

        self.db.cur.execute(
            RegExp.space.sub(" ", """
                INSERT INTO t_post_info(parent_id, external_post_id, ref_source, ref_type, ref_account, content, datetime)
                VALUES (%(p_parent_id)s, %(p_post_id)s, %(p_src_id)s, %(p_type_id)s, %(p_account_id)s, %(p_content)s, %(p_datetime)s)
                RETURNING id
            """),
            {
                "p_parent_id": parent_id,
                "p_post_id": item.get("post_id", None),
                "p_src_id": self._source_id,
                "p_type_id": post_type,
                "p_account_id": account_id,
                "p_content": content,
                "p_datetime": item.get("datetime", None)
            }
        )
        return self.db.cur.fetchone()[0]
