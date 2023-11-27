import mechanicalsoup
import redis
import configparser
from elasticsearch import Elasticsearch, helpers

import pandas as pd
import numpy as np

config = configparser.ConfigParser()
config.read('example.ini')
# print(config.read('example.ini')))
# ['example.ini]

es = Elasticsearch(
    cloud_id=config['ELASTIC']['cloud_id'],
    basic_auth=(config['ELASTIC']['user'], config['ELASTIC']['password'])
)

def write_to_elastic(es, url, html):
    link = url.decode('utf-8')
    es.index(index='webpages', document={'url': link, 'html':html})

def crawl(browser, r, es, url):
    # Download url
    print("Downloading page...")
    browser.open(url)

    # Cache page to elasticsearch
    write_to_elastic(es, url, str(browser.page))

    # Parse for more urls
    print("Parsing for more links")
    a_tags = browser.page.find_all('a')
    hrefs = [link.get('href') for link in a_tags]
    
    # could change filtering based on the site
    wikipedia_domain = "https://en.wikipedia.org"
    print('Parsing webpage for links...')
    links = [wikipedia_domain + href for href in hrefs if href and href.startswith("/wiki/")]

    # Pur urls in redits queue
    print('Saving links to Redis...')
    r.lpush('links', *links)

browser = mechanicalsoup.StatefulBrowser()
r = redis.Redis()
r.flushall()

start_url = "https://en.wikipedia.org/wiki/Redis"
r.lpush("links", start_url)

while link := r.rpop('links'):
    if "Jesus" in str(link):
        break
    crawl(browser, r, es, link)