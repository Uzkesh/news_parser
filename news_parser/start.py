from scrapy.utils.project import get_project_settings
from scrapy.crawler import CrawlerProcess
from news_parser.spiders.bankiru import BankiruSpider
from news_parser.spiders.bankiru_clients import BankiruClientsSpider
from news_parser.spiders.pikabu import PikabuSpider
from news_parser.reports.reports import Report, RParamsDTO, RPostAutoParsingDTO
from news_parser.mailers.email import EmailManager
from datetime import datetime


# TODO: Добавить разбор входных параметров


def start():
    limit_date = datetime.strptime("01.05.2020", "%d.%m.%Y")

    process = CrawlerProcess(get_project_settings())
    process.crawl(BankiruSpider, limit_date=limit_date)
    process.crawl(BankiruClientsSpider, limit_date=limit_date)
    process.crawl(PikabuSpider, limit_date=limit_date)
    process.start()

    report_worker = Report()
    report_worker.set_params(RParamsDTO(
        recipient_id=0,
        main=RPostAutoParsingDTO(
            datetime_begin=datetime.now(),
            datetime_end=datetime.now()
        )
    ))
    fname, recipients = report_worker.generate_report()

    EmailManager.send_report(fname, recipients)


start()
