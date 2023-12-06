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

    def flush_db(self):
        print("clearing graph db")
        with self.driver.session() as session:
            session.execute_write(self._flush_db)

    @staticmethod
    def _create_links(tx, page, links):
        page = page.decode('utf-8')
        tx.run("CREATE (:Page {url: $page})", page=page)
        for link in links:
            tx.run("MATCH (p:Page) WHERE p.url = $page "
                "CREATE (:Page {url: $link}) -[:LINKS_TO]-> (p)",
                link=link, page=page)
            # tx.run("CREATE (:Page {url: $link}) -[:LINKS_TO]-> (:Page {url: $page})",
            #     link=link, page=page.decode('utf-8'))

    @staticmethod
    def _flush_db(tx):
        tx.run("MATCH (a) -[r]-> () DELETE a, r")
        tx.run("MATCH (a) DELETE a")


neo4j_connector = Neo4JConnector("bolt://localhost:7687", "neo4j", "Puffles12")
neo4j_connector.flush_db()

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