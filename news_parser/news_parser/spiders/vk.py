from news_parser.settings import SPIDER_URLS, TOKENS
from news_parser.items import Post, Comment
from time import ctime
from typing import Optional
import scrapy
import re
import vk


class VKSpider(scrapy.Spider):
    name = "vk"
    start_urls = SPIDER_URLS[name]

    custom_settings = {
        "USER_AGENT": None,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.api = vk.API(vk.Session(TOKENS[self.name]))
        self.last_part = 0
        self.last_ntime = 0
        self.completed = False

    def set_last_post_id(self, post_id: int):
        if post_id is None:
            self.last_part, self.last_ntime = 0, 0
        else:
            str_post_id = str(post_id)
            cnt = int(str_post_id[-1])
            self.last_part = int(str_post_id[:cnt])
            self.last_ntime = int(str_post_id[cnt:-1])

    def parse(self, response):
        res = list()
        group_id = -84354128
        posts = self.api.wall.get(owner_id=group_id, filter='owner', count=10, extended=1, v=5.107)["items"][0]
        for post in posts:
            res.append(Post(
                post_url=f"https://vk.com/wall{group_id}_{post['id']}",
                post_id=post["id"],
                title=None,
                rating=post["likes"]["count"],
                msg=post["text"],
                bank_answer=None,
                author_uid=post["from_id"] if post["from_id"] > 0 else None,
                author_login=post["profiles"]["screen_name"] if post["from_id"] > 0 else None,
                datetime=ctime(post["date"]),
                comments=self.parse_comments(group_id, post["id"], None)
            ))

    def parse_comments(self, owner_id: int, post_id: int, comment_id: Optional[int], pack=100, offset=0):
        res = list()

        return res
