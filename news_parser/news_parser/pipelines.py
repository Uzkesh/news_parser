# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

from .common.db_manager import DB, TypeDBParams
import re


class NewsParserPipeline(object):
    def __init__(self, params: TypeDBParams):
        self._params = params
        self.db = None

    @classmethod
    def from_crawler(cls, crawler):
        db_params = crawler.settings.getdict("DB_PARAMS")
        return cls(TypeDBParams(name=db_params["name"]))

    def open_spider(self, spider):
        self.db = DB(self._params)
        self._get_last_record_ids(spider)

    def close_spider(self, spider):
        self.db.disconnect()

    def process_item(self, item, spider):
        if spider.name == "vk":
            self.db.cur.execute(re.sub(r"(\s)+", " ", """
                    INSERT INTO main.t_vk_posts(nday, ntime, msg)
                    VALUES (:p_nday, :p_ntime, :p_msg)
                """),
                {
                    "p_nday": item["nday"],
                    "p_ntime": item["ntime"],
                    "p_msg": item["msg"]
                }
            )
            self.db.commit()

        return item

    def _get_last_record_ids(self, spider):
        if spider.name == "vk":
            self.db.cur.execute(re.sub(r"(\s)+", " ", """
                SELECT v.last_nday
                     , max(ntime) as last_ntime
                  FROM t_vk_posts tbl
                 INNER JOIN (SELECT max(nday) as last_nday
                               FROM t_vk_posts) v
                    ON tbl.nday = v.last_nday
            """))

            record = self.db.cur.fetchone()
            spider.last_nday = int(record[0] or 0)
            spider.last_ntime = int(record[1] or 0)


