import mechanicalsoup
import redis
import configparser
from elasticsearch import Elasticsearch, helpers
from neo4j import GraphDatabase

class Neo4JConnector:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()
    
    def add_links(self, page, links):
        with self.driver.session() as session:
            session.execute_write(self._create_links, page, links)

    @staticmethod
    def _create_links(tx, page, links):
        for link in links:
            tx.run("CREATE (:Page {url: $link}) -[:LINKS_TO]-> (:Page {url: $page})",
                page=page, link=str(link))


neo4j_connector = Neo4JConnector("bolt://localhost:7687", "neo4j", "Puffles12")

config = configparser.ConfigParser()
config.read('example.ini')

es = Elasticsearch(
    cloud_id=config['ELASTIC']['cloud_id'],
    basic_auth=(config['ELASTIC']['user'], config['ELASTIC']['password'])
)

def write_to_elastic(es, url, html):
    es.index(index='webpages', document={'url': link, 'html':html})

def crawl(browser, r, url):
    print("Downloading page...")
    browser.open(url)

    a_tags = browser.page.find_all('a')
    hrefs = [link.get('href') for link in a_tags]
    
    # could change filtering based on the site
    wikipedia_domain = "https://en.wikipedia.org"
    print('Parsing webpage for links...')
    links = [wikipedia_domain + href for href in hrefs if href and href.startswith("/wiki/")]

    print('Saving links to Redis...')
    r.lpush('links', *links)
    new_link = r.rpop
    print(new_link)\
    
    neo4j_connector.add_links(url, links)

def add_links(self, page, links):
    with self.driver.session() as session:
        session.execute_write(self._create_links, page, links)

def print_greeting(self, message):
    with self.driver.session() as session:
        greeting = session.execute_write(self._create_and_return_greeting, message)
        print(greeting)


browser = mechanicalsoup.StatefulBrowser()
r = redis.Redis()
r.flushall()

start_url = "https://en.wikipedia.org/wiki/Redis"
r.lpush("links", start_url)

while link := r.rpop('links'):
    if "Jesus" in str(link):
        print("Found Jesus!")
        break
    crawl(browser, r, link)