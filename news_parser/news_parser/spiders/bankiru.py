from news_parser.items import PostBankiru, CommentBankiru
from news_parser.common.regexp_template import RegExp
from news_parser.settings import SPIDER_URLS
from datetime import datetime
import scrapy
import re


class BankiruSpider(scrapy.Spider):
    name = "bankiru"
    start_urls = SPIDER_URLS[name]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.last_post_id = 11190183
        self.completed = False

    def last_post_id(self, post_id: str):
        # self.last_post_id = int(post_id)
        self.last_post_id = 11190183

    def parse(self, response):
        main = response.css("main.layout-column-center")
        posts = main.css("table.resptab")

        for post in posts:
            post_link = re.sub(r"#.+$", "", post.css("a.linkNote::attr(href)").get())
            yield response.follow(f"{post_link}/", self.parse_post)
            if self.completed:
                break

        current_page = response.url.split("/")[-1]
        lst_url = current_page.split("=")
        if len(lst_url) > 2:
            next_page = f"{lst_url[0]}={lst_url[1]}={int(lst_url[2]) + 1}"
        else:
            next_page = f"{current_page}&PAGEN_1=2"

        yield dict() if self.completed else response.follow(next_page, callback=self.parse)

    def parse_post(self, response):
        post_id = int(response.url.split("/")[-2])
        bank_answer_id = f"block_text_{post_id}"

        if post_id <= self.last_post_id:
            self.completed = True

        post = response.css("table.resptab")
        author = response.css("table.resptab td.footerline a")
        comments = response.css("[id='comments-items-wrapper'] div.elementMessage")

        title = RegExp.space.sub(" ", post.css("td.headerline::text").get().strip())
        rating = re.search(r"[-−]?[0-9]+", post.css("td.rating nobr::text").get() or "") or post.css("td.rating::text").get()
        rating = RegExp.space.sub(" ", rating.group(0).replace("−", "-") if type(rating) is re.Match else rating).strip()
        msg = RegExp.space.sub(" ", RegExp.tag.sub(" ", post.css("td.article-text").get() or "").strip())
        bank_answer = RegExp.space.sub(" ", RegExp.tag.sub(" ", post.css(f"[id='{bank_answer_id}']").get() or "").strip())
        author_uid = author.attrib["href"].split("=")[-1]
        author_login = author.css("::text").get()
        dt = datetime.strptime(response.css("span.color-grey::text").get(), "%d.%m.%Y %H:%M")

        comments_data = list()
        for comment in comments:
            comment_author_uid = comment.css("a.userName::attr(href)").get()
            comment_author_uid = comment_author_uid.split("=")[-1] if comment_author_uid else None
            comment_author_login = comment.css("a.userName::text").get() or comment.css("td.userinfo strong::text").get()

            comments_data.append(CommentBankiru(
                author_uid=comment_author_uid,
                author_login=comment_author_login,
                datetime=datetime.strptime(comment.css("div.pressmon::text").get(), "%d.%m.%Y %H:%M"),
                msg=RegExp.space.sub(" ", RegExp.tag.sub(" ", comment.css("td.article-text").get()).strip())
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
