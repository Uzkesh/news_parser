# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class NewsParserItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass


class PostBankiru(scrapy.Item):
    post_id = scrapy.Field()
    title = scrapy.Field()
    rating = scrapy.Field()
    msg = scrapy.Field()
    bank_answer = scrapy.Field()
    author_uid = scrapy.Field()
    author_login = scrapy.Field()
    datetime = scrapy.Field()
    comments = scrapy.Field()


class CommentBankiru(scrapy.Item):
    author_uid = scrapy.Field()
    author_login = scrapy.Field()
    datetime = scrapy.Field()
    msg = scrapy.Field()
