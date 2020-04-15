import scrapy
import re


class VKSpider(scrapy.Spider):
    name = "vk"
    start_urls = ["https://m.vk.com/sber.sluh"]
    last_nday = None or 0
    last_ntime = None or 0

    def parse(self, response):
        completed = False
        posts = response.css("div.wall_item")

        for post in posts:
            post_id = post.css("a.post__anchor.anchor::attr(name)")[0].get()

            nday, ntime = post_id.replace("-", "_").split("_")[1:]
            msg = re.sub(r"(\s)+", " ", re.sub(r"<[^<]+>", "",
                post.css("div.wi_body div.pi_text").get() or "").strip()
            )

            completed = int(nday) < self.last_nday or (int(nday) == self.last_nday and int(ntime) <= self.last_ntime)
            if completed:
                break

            yield {
                "nday": nday,
                "ntime": ntime,
                "msg": msg
            }

        yield from dict() if completed else response.follow_all(
            css="div.wall_posts div.show_more_wrap a",
            callback=self.parse
        )
