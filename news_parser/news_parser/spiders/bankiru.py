import scrapy


class BankiruSpider(scrapy.Spider):
    name = "bankiru"
    start_urls = ["https://www.banki.ru/services/official/bank/?ID=322"]

    def parse(self, response):
        n = len(response.css("main.layout-column-center").getall())
        print(n)
        yield n
