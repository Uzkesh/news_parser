from news_parser.reports.reports import Report, RParamsDTO, RPostAutoParsingDTO
from news_parser.mailers.email import EmailManager
from datetime import datetime
import os


def start():
    os.system("scrapy crawl bankiru")
    os.system("scrapy crawl bankiru_clients")
    os.system("scrapy crawl pikabu")

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
