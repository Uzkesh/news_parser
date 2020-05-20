from news_parser.items import Post, Comment
from news_parser.common.regexp_template import RegExp
from news_parser.settings import SPIDER_URLS
from datetime import datetime
import scrapy
import re


class PikabuSpider(scrapy.Spider):
    name = "pikabu"
    start_urls = SPIDER_URLS[name]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.limit_post_id = kwargs.get("limit_post_id", None)
        self.limit_date = kwargs.get("limit_date", None)
        self.completed = False

    def set_last_post_id(self, post_id: int):
        self.limit_post_id = self.limit_post_id or post_id

    def parse(self, response):
        posts = response.css("article.story")

        for post in posts:
            flag_sponsor_post = post.css("a.story__sponsor.story__sponsor_bottom::attr(href)").get()
            if flag_sponsor_post is None:
                post_link = post.css("div.story__main header.story__header h2.story__title a::attr(href)").get()
                if post_link:
                    yield response.follow(post_link, self.parse_post)

            if self.completed:
                break

        current_page = response.url.split("/")[-1]
        lst_url = current_page.split("=")
        if len(lst_url) > 1:
            next_page = f"{lst_url[0]}={int(lst_url[1]) + 1}"
        else:
            next_page = f"{current_page}?page=2"

        yield dict() if self.completed else response.follow(next_page, callback=self.parse)

    def parse_post(self, response):
        out_of_limit = False
        post_url = response.url
        post_id = int(post_url.split("_")[-1])

        post = response.css("div.page-story div.story__main")
        footer = response.css("div.story__footer")

        title = RegExp.space.sub(" ", post.css("header.story__header span.story__title-link::text").get().strip())
        msg = RegExp.space.sub(" ", RegExp.tag.sub(" ", post.css("div.story__content.story__typography div.story__content-inner").get() or "").strip())
        author_uid = response.css(f"article.story[data-story-id='{post_id}']").attrib["data-author-id"]
        author_login = footer.css("a.user__nick.story__user-link::text").get()
        dt = datetime.strptime(re.sub(":00$", "00", footer.css("time.caption.story__datetime.hint").attrib["datetime"]), "%Y-%m-%dT%H:%M:%S%z")

        if self.limit_date and dt < self.limit_date or self.limit_post_id and post_id <= self.limit_post_id:
            self.completed = True
            out_of_limit = True

        yield Post() if out_of_limit else Post(
            post_url=post_url,
            post_id=post_id,
            title=title,
            rating=None,
            msg=msg,
            bank_answer=None,
            author_uid=author_uid,
            author_login=author_login,
            datetime=dt,
            comments=list()
        )
