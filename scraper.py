"""Test scraper."""

import grequests
import redis
from bs4 import BeautifulSoup as bs

import logging
import sys
import json


class RawWebCrawler(object):

    def __init__(self, redis_host=None, redis_port=6379, 
                 loglevel=logging.DEBUG, async_chunks=200, crawl_depth=2):

        self.redis_conn = redis.StrictRedis(redis_host, redis_port)

        genlog = logging.getLogger()
        genlog.setLevel(loglevel)
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(loglevel)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        genlog.addHandler(ch)

        self.logger = genlog

        self.async_chunks = async_chunks
        self.crawl_depth = crawl_depth

    def get_job(self):
        """Method used to get a new job from the job queue."""
        pass

    def post_results(self, msg):
        """Method used to post a result to the result queue."""
        pass

    def scrape_urls(self, urls):
        """Method used to async fetch all pages in the given urls."""
        # TODO: return unscraped urls to queue
        errors = []

        def exception_handler(request, exception):
            self.logger.error("Problem scraping %s: %s", (request.url, exception))
            errors.append(request.url)

        chunk_size = self.async_chunks
        urls = list(urls)
        # if too many, split in chunks to avoid IO problems
        if len(urls) > chunk_size:
            chunks = [urls[x:x + min(chunk_size, len(urls) - x)]
                      for x in range(0, len(urls), chunk_size)]
        else:
            chunks = [urls]

        results = []
        for chunk in chunks:
            rs = (grequests.get(u) for u in chunk)
            responses = grequests.map(rs, exception_handler=exception_handler)
            # TODO: add error handling for each url
            for resp in responses:
                if resp is not None and resp.status_code == 200:
                    results.append({"content": resp.content,
                                    "url": resp.url,
                                    "elapsed": resp.elapsed})

        self.logger.debug("scraped %s pages" % len(results))
        return results, errors

    def check_scraped(self, url, baseurl):
        """Method used to check if a given url has been already scraped."""
        test = self.redis_conn.hexists(baseurl, url.replace(baseurl, ""))
        if not test:
            self.redis_conn.hset(baseurl, url.replace(baseurl, ""), 1)
            return False
        else:
            return True

    def validate_url(self, url, baseurl):
        """Method used to validate a given url."""
        suffix = not any([file_sfx in url for file_sfx in
                          ["css", "js", "png", "gif", "jpg", "jpeg"]])
        specs = not any([spc in url for spc in
                         ["/tags/", "/print/", "mailto", "/rss", "#"]])
        domain = True if url.startswith(baseurl) else False

        return suffix and domain and specs

    def sanitize_urls(self, urls, baseurl):
        """Method used to sanitize and perform checks on a set of urls."""
        sanitized = []
        self.logger.debug("received %s urls for sanitization" % len(urls))
        for entry in urls:
            if entry.startswith("/"):
                entry = baseurl + entry
            if self.validate_url(entry, baseurl) and not self.check_scraped(entry, baseurl):
                sanitized.append(entry)

        self.logger.debug("after sanitization: %s urls \n *******" % len(sanitized))
        return set(sanitized)

    def extract_links(self, content):
        """Method used to extract all links from a list of pages."""
        links = []
        for entry in content:
            try:
                soup = bs(entry, "html.parser")
                extracted = soup.find_all(href=True)
                for tag in extracted:
                    links.append(tag["href"])
            except Exception:
                self.logger.exception()

        self.logger.debug("extracted %s links" % len(links))
        return set(links)

    def run(self, sources=None, to_file=None, to_redis=None):
        """Main loop."""
        depth = self.crawl_depth

        if sources is not None:
            if not isinstance(sources, list):
                seeds = [sources]

        for item in seeds:
            results, errors = self.scrape_urls([item])
            if errors and sources is None:
                # TODO: return these urls to the queue
                self.logger.debug('problems scraping some urls.')

            to_scrape = self.sanitize_urls(self.extract_links(results), item)

            while depth > 0:
                results = self.scrape_urls(to_scrape)

                if to_redis is not None and results:
                    self.redis_conn.lpush(to_redis, *results)
                if to_file is not None and results:
                    with open(to_file, "a") as f:
                        json.dump(f, results)

                just_pages = [item["content"] for item in results]
                links = self.sanitize_urls(self.extract_links(just_pages), item)

                to_scrape = links
                depth -= 1


if __name__ == "__main__":
    sources = "http://www.zoso.ro"
    crawler = RawWebCrawler(redis_host="0.0.0.0", redis_port=32769)
    crawler.run(sources, to_redis="scraped:results")
