from news_parser.settings import SPIDER_URLS, TOKENS
from news_parser.items import Post, Comment, PostContainer
from datetime import datetime
from typing import Optional
import scrapy
import re
import vk


class VKSpider(scrapy.Spider):
    name = "vk"
    start_urls = SPIDER_URLS[name]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.limit_date = kwargs.get("limit_date", None)
        self.api = vk.API(vk.Session(TOKENS[self.name]))
        self.api_version = 5.107
        # TODO: Получать из БД список каналов (доработать структуру ref_source)
        self.sources = [-84354128]
        self.count = 100
        self.last_part = 0
        self.last_ntime = 0
        self.completed = False

    def set_last_post_id(self, post_id: int):
        if post_id is None:
            self.last_part, self.last_ntime = 0, 0
        # else:
        #     str_post_id = str(post_id)
        #     cnt = int(str_post_id[-1])
        #     self.last_part = int(str_post_id[:cnt])
        #     self.last_ntime = int(str_post_id[cnt:-1])

    # TODO: Убрать переходы по страницам - оставить только цикл
    def parse(self, response):
        url = response.url
        url_parts = url.split("?")
        base_url = url_parts[0]
        if len(url_parts) > 1:
            offset = int((re.search(r"offset=(\d)+", url_parts[1]) or ["=0"])[0].split("=")[1])
            count = int((re.search(r"count=(\d)+", url_parts[1]) or ["=100"])[0].split("=")[1])
        else:
            offset = 0
            count = 100

        next_offset = offset + count
        next_page = f"{base_url}?offset={next_offset}&count={count}"

        # TODO: надо сделать флаг completed для каждого source отдельно
        for source in self.sources:
            if not self.completed:
                yield PostContainer(data=self.parse_posts(owner_id=source, offset=offset, count=count))

        yield dict() if self.completed else response.follow(next_page, callback=self.parse)

    def parse_posts(self, owner_id: int, offset: int, count: int) -> list:
        res = list()

        posts_info = self.api.wall.get(
            owner_id=owner_id,
            filter='owner',
            count=count,
            offset=offset,
            extended=1,
            v=self.api_version
        )
        posts = posts_info.get("items", list())
        profiles = {item["id"]: item.get("screen_name", item["first_name"]) for item in posts_info.get("profiles", list())}
        profiles.update({-item["id"]: item["screen_name"] for item in posts_info.get("groups", list())})

        for post in posts:
            post_date = datetime.fromtimestamp(post["date"])

            if self.limit_date and post_date < self.limit_date:
                self.completed = True
                break

            res.append(Post(
                post_url=f"https://vk.com/wall{owner_id}_{post['id']}",
                post_id=post["id"],
                title=None,
                rating=post["likes"]["count"],
                msg=post["text"],
                bank_answer=None,
                author_uid=post["from_id"],
                author_login=profiles[post["from_id"]],
                datetime=post_date,
                comments=self.parse_comments(
                    owner_id=owner_id,
                    post_id=post["id"],
                    comment_id=None,
                    offset=0,
                    count=100
                )
            ))

        # if len(res) == count:
        #     res += self.parse_posts(
        #         owner_id=owner_id,
        #         offset=offset + count
        #     )

        return res

    def parse_comments(self, owner_id: int, post_id: int, comment_id: Optional[int], offset: int, count: int) -> list:
        res = list()

        comments_info = self.api.wall.getComments(
            owner_id=owner_id,
            post_id=post_id,
            comment_id=comment_id,
            sort="asc",
            count=count,
            offset=offset,
            extended=1,
            v=self.api_version
        )
        comments = comments_info.get("items", list())
        profiles = {item["id"]: item.get("screen_name", item["first_name"]) for item in comments_info.get("profiles", list())}
        profiles.update({-item["id"]: item["screen_name"] for item in comments_info.get("groups", list())})

        # Парсинг сомментов
        for comment in comments:
            if not comment.get("deleted", None):
                res.append(Comment(
                    author_uid=comment["from_id"],
                    author_login=profiles[comment["from_id"]],
                    datetime=datetime.fromtimestamp(comment["date"]),
                    msg=comment.get("text", "Комментарий удален"),
                    comments=self.parse_comments(
                        owner_id=owner_id,
                        post_id=post_id,
                        comment_id=comment["id"],
                        offset=0,
                        count=count
                    ) if comment.get("thread", {"count": 0})["count"] > 0 else list()
                ))

        # Продолжить сбор комментов, если их больше count
        if len(res) == count:
            res += self.parse_comments(
                owner_id=owner_id,
                post_id=post_id,
                comment_id=comment_id,
                offset=offset + count,
                count=count
            )

        return res
