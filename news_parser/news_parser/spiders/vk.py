from news_parser.settings import SPIDER_URLS
import scrapy
import re


class VKSpider(scrapy.Spider):
    name = "vk"
    start_urls = SPIDER_URLS[name]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.last_part = None
        self.last_ntime = None
        self.completed = False

    def last_post_id(self, post_id: str):
        if post_id == "0":
            self.last_part, self.last_ntime = 0, 0
        else:
            cnt = int(post_id[-1])
            self.last_part = int(post_id[:cnt])
            self.last_ntime = int((post_id[cnt:])[:-1])

    def parse(self, response):
        posts = response.css("div.wall_item")

        for post in posts:
            post_id = post.css("a.post__anchor.anchor::attr(name)")[0].get()

            dt = post_id.replace("-", "_").split("_")[1:]
            part, ntime = int(dt[0]), int(dt[1])
            msg = re.sub(r"(\s)+", " ", re.sub(r"<[^<]+>", "",
                post.css("div.wi_body div.pi_text").get() or "").strip()
            )

            self.completed = part < self.last_part or (part == self.last_part and ntime <= self.last_ntime)
            if self.completed:
                break

            yield {
                "part": part,
                "ntime": ntime,
                "msg": msg
            }

        yield from dict() if self.completed else response.follow_all(
            css="div.wall_posts div.show_more_wrap a",
            callback=self.parse
        )
