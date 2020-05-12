FROM python:latest

RUN mkdir -p /usr/src/app/
WORKDIR /usr/src/app/
COPY . /usr/src/app/

#RUN apt-get install python3-dev libpq-dev
RUN pip install --no-cache-dir -r requirements.txt

#CMD ["scrapy", "crawl", "bankiru"]
