import scrapy
import re


class VKSpider(scrapy.Spider):
    name = "vk"
    start_urls = ["https://m.vk.com/sber.sluh"]
    last_part = None
    last_ntime = None

    def parse(self, response):
        completed = False
        posts = response.css("div.wall_item")

        for post in posts:
            post_id = post.css("a.post__anchor.anchor::attr(name)")[0].get()

            dt = post_id.replace("-", "_").split("_")[1:]
            part, ntime = int(dt[0]), int(dt[1])
            msg = re.sub(r"(\s)+", " ", re.sub(r"<[^<]+>", "",
                post.css("div.wi_body div.pi_text").get() or "").strip()
            )

            completed = part < self.last_part or (part == self.last_part and ntime <= self.last_ntime)
            if completed:
                break

            yield {
                "part": part,
                "ntime": ntime,
                "msg": msg
            }

        yield from dict() if completed else response.follow_all(
            css="div.wall_posts div.show_more_wrap a",
            callback=self.parse
        )
