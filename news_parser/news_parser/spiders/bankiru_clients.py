from news_parser.items import PostBankiru, CommentBankiru
from news_parser.common.regexp_template import RegExp
from news_parser.settings import SPIDER_URLS
from datetime import datetime
import scrapy
import re
import json


class BankiruClientsSpider(scrapy.Spider):
    name = "bankiru_clients"
    start_urls = SPIDER_URLS[name]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.last_post_id = 10354130
        self.completed = False

    def last_post_id(self, post_id: str):
        # self.last_post_id = int(post_id)
        self.last_post_id = 10354130

    def parse(self, response):
        posts = response.css("article.responses__item")

        for post in posts:
            post_link = post.css("a.header-h3::attr(href)").get()
            yield response.follow(post_link, self.parse_post)
            if self.completed:
                break

        lst_url = response.url.split("=")
        next_page = f"{lst_url[0]}={int(lst_url[1]) + 1}"

        yield dict() if self.completed else response.follow(next_page, callback=self.parse)

    def parse_post(self, response):
        post_id = int(response.url.split("/")[-2])

        if post_id <= self.last_post_id:
            self.completed = True

        post = response.css("article.response-page")
        answers = response.css("div.response-thread")
        raw_comments = re.search("prefetchData: \[\{.+}]\n", response.css("main.layout-column-center").get())
        comments = json.loads(RegExp.space.sub(" ", raw_comments.group(0).split("prefetchData: ")[1])) if type(raw_comments) is re.Match else list()

        title = RegExp.space.sub(" ", post.css("h0.header-h0.response-page__title::text").get().strip())
        msg = RegExp.space.sub(" ", RegExp.tag.sub(" ", post.css("div[data-test='responses-message']").get() or "").strip())
        author_uid = post.css("a[data-test='responses-user-link']::attr(href)").get().split("=")[-1]
        author_login = post.css("span[itemprop='reviewer']::text").get()
        dt = datetime.strptime(post.css("time[itemprop='dtreviewed']").attrib["datetime"], "%Y-%m-%d %H:%M:%S")

        bank_answer = RegExp.space.sub(" ", RegExp.tag.sub(" ", answers.css("[id='bankAnswer'] script[data-name='answer-text']::text").get() or "").strip())
        rating = RegExp.space.sub(" ", post.css("span[itemprop='ratingValue']::text").get() or "").strip()

        comments_data = list()
        for comment in comments:
            comments_data.append(CommentBankiru(
                author_uid=comment.get("authorId", None),
                author_login=comment["author"],
                datetime=comment["dateCreate"],
                msg=RegExp.tag.sub("", re.sub(
                    "<blockquote class=\"quote\">", ">> ", re.sub(
                        "</blockquote>(\s)+", "\n\n", RegExp.space.sub(" ", comment["text"]).strip()
                    )
                ))
            ))

        yield PostBankiru() if self.completed else PostBankiru(
            post_id=post_id,
            title=title,
            rating=rating,
            msg=msg,
            bank_answer=bank_answer,
            author_uid=author_uid,
            author_login=author_login,
            datetime=dt,
            comments=comments_data
        )
