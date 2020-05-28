# -*- coding: utf-8 -*-

# Scrapy settings for news_parser project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html

import os

BOT_NAME = 'news_parser'

SPIDER_MODULES = ['news_parser.spiders']
NEWSPIDER_MODULE = 'news_parser.spiders'


# Crawl responsibly by identifying yourself (and your website) on the user-agent
USER_AGENT = os.environ['USER_AGENT']

# Obey robots.txt rules
ROBOTSTXT_OBEY = os.environ['ROBOTS_FL']

# Configure maximum concurrent requests performed by Scrapy (default: 16)
#CONCURRENT_REQUESTS = 32

# Configure a delay for requests for the same website (default: 0)
# See https://docs.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
DOWNLOAD_DELAY = os.environ['DOWNLOAD_DELAY']
# The download delay setting will honor only one of:
#CONCURRENT_REQUESTS_PER_DOMAIN = 16
#CONCURRENT_REQUESTS_PER_IP = 16

# Disable cookies (enabled by default)
COOKIES_ENABLED = os.environ['COOKIES_ENABLED']

# Disable Telnet Console (enabled by default)
#TELNETCONSOLE_ENABLED = False

# Override the default request headers:
#DEFAULT_REQUEST_HEADERS = {
#   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
#   'Accept-Language': 'en',
#}

# Enable or disable spider middlewares
# See https://docs.scrapy.org/en/latest/topics/spider-middleware.html
#SPIDER_MIDDLEWARES = {
#    'news_parser.middlewares.NewsParserSpiderMiddleware': 543,
#}

# Enable or disable downloader middlewares
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#DOWNLOADER_MIDDLEWARES = {
#    'news_parser.middlewares.NewsParserDownloaderMiddleware': 543,
#}

# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
#EXTENSIONS = {
#    'scrapy.extensions.telnet.TelnetConsole': None,
#}

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
ITEM_PIPELINES = {
    'news_parser.pipelines.NewsParserPipeline': 300,
}

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/autothrottle.html
#AUTOTHROTTLE_ENABLED = True
# The initial download delay
#AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
#AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
#AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
#AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
#HTTPCACHE_ENABLED = True
#HTTPCACHE_EXPIRATION_SECS = 0
#HTTPCACHE_DIR = 'httpcache'
#HTTPCACHE_IGNORE_HTTP_CODES = []
#HTTPCACHE_STORAGE = 'scrapy.extensions.httpcache.FilesystemCacheStorage'

DB_PARAMS = {
    "host": os.environ['DB_HOST'],
    "port": os.environ['DB_PORT'],
    "name": os.environ['DB_NAME'],
    "user": os.environ['DB_USER'],
    "password": os.environ['DB_PASSWORD'],
}

SPIDER_URLS = {
    "bankiru": [os.environ["BANKIRU_SPIDER_URL"]],
    "bankiru_clients": [os.environ["BANKIRU_CLIENTS_SPIDER_URL"]],
    "pikabu": [os.environ["PIKABU_SPIDER_URL"]],
    "vk": [os.environ["VK_SPIDER_URL"]]
}

EMAIL_MANAGER = {
    "login": os.environ["EMAIL_MANAGER_LOGIN"],
    "password": os.environ["EMAIL_MANAGER_PASSWORD"]
}

TOKENS = {
    "vk": os.environ["TOKEN_VK"]
}
