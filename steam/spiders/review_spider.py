import re
import urllib.parse
import scrapy
from scrapy.http import FormRequest, Request
from w3lib.url import url_query_parameter

from ..items import ReviewItem, ReviewItemLoader, str_to_int
from ..sqlite import Database

import logging


def load_review(review, product_id, page, order, response):
    """
    Load a ReviewItem from a single review.
    """
    loader = ReviewItemLoader(ReviewItem(), review)

    loader.add_value('product_id', product_id)
    loader.add_value('page', page)
    loader.add_value('page_order', order)

    # Review data.
    loader.add_css('recommended', '.title::text')
    loader.add_css('date', '.date_posted::text', re='Posted: (.+)')
    text = loader.get_css('.apphub_CardTextContent::text')
    loader.add_css('text', '.apphub_CardTextContent::text')
    loader.add_css('hours', '.hours::text', re='(.+) hrs')
    loader.add_css('compensation', '.received_compensation::text')

    # User/reviewer data.
    user_id = loader.get_css('.apphub_CardContentAuthorName a::attr(href)', re='.*/profiles/(.+)/')
    if not user_id:
        user_id = loader.get_css('.apphub_CardContentAuthorName a::attr(href)', re='.*/id/(.+)/')

    loader.add_value('user_id', user_id[0])
    loader.add_css('username', '.apphub_CardContentAuthorName a::text')
    if not user_id or not text:
        with open(f'review_fails/{product_id}-p{page}.html', 'w') as wf:
            wf.write(response.text)
    loader.add_css('products', '.apphub_CardContentMoreLink ::text', re='([\d,]+) product')

    # Review feedback data.
    feedback = loader.get_css('.found_helpful ::text')
    loader.add_value('found_helpful', feedback, re='([\d,]+).*helpful')
    loader.add_value('found_funny', feedback, re='([\d,]+).*funny')
    awarding = loader.get_css('.review_award_aggregated ::text')
    loader.add_value('found_awarding', awarding)

    early_access = loader.get_css('.early_access_review')
    if early_access:
        loader.add_value('early_access', True)
    else:
        loader.add_value('early_access', False)

    loader.add_value('url', response.url)

    return loader.load_item()


def get_page(response):
    from_page = response.meta.get('from_page', None)

    if from_page:
        page = from_page + 1
    else:
        page = url_query_parameter(response.url, 'p', None)
        if page:
            page = str_to_int(page)

    return page


def get_product_id(response):
    product_id = response.meta.get('product_id', None)

    if not product_id:
        try:
            return re.findall("app/(.+?)/", response.url)[0]
        except Exception as e:
            try:
                return re.findall("app/(.+?)$", response.url)[0]
            except:
                return None
    else:
        return product_id


class ReviewSpider(scrapy.Spider):
    name = 'reviews'
    test_urls = [
        # Full Metal Furies
        'http://steamcommunity.com/app/416600/reviews/?browsefilter=mostrecent&p=1',
    ]
    custom_settings = {
        'DOWNLOADER_MIDDLEWARES': {
            'steam.middlewares.AddAgeCheckCookieMiddleware': None,
        }
    }

    def __init__(self, sqlite_path=None, steam_id=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        logging.log(logging.INFO, 'Loading database.')
        self.db = Database(sqlite_path, False)
        self.steam_id = steam_id
        # self.db.delete_partially_processed_reviews()
        self.partially_processed_product_urls = self.db.get_last_urls_from_partially_processed_products()
        self.unprocessed_products = self.db.get_products_with_unprocessed_reviews()

        self.review_ids = self.db.get_review_ids()
        self.rscrape_ids = self.db.get_rscrape_ids()
        self.user_ids = self.db.get_user_ids()
        logging.log(logging.INFO, 'Database loaded.')

    def read_urls(self):
        for product_id, url in self.unprocessed_products:
            yield scrapy.Request(url, cookies={"wants_mature_content_apps": product_id}, callback=self.parse)

    def start_requests(self):
        # first go over partially processed products
        for url in self.partially_processed_product_urls:
            # test if URL is in proper format
            regex_homecontent = re.match(r"^.*app/(\d+)/homecontent/.*$", url)
            regex_review = re.match(r"^.*app/(\d+)/reviews/.*$", url)

            if bool(regex_homecontent):
                yield self.form_request_from_last_url(url)
            elif regex_review:
                self.steam_id = re.findall(r"^.*app/(\d+)/reviews/.*$", url)[0]
        if self.steam_id:
            url = (
                f'http://steamcommunity.com/app/{self.steam_id}/reviews/'
                '?browsefilter=mostrecent&p=1'
            )
            yield Request(url, callback=self.parse)
        elif self.unprocessed_products:
            yield from self.read_urls()
        else:
            for url in self.test_urls:
                yield Request(url, callback=self.parse)

    def parse(self, response, **kwargs):
        page = get_page(response)
        product_id = get_product_id(response)

        # Handle redirects (usually for DLCs where review links don't work).
        if 'reviews' not in response.url and response.meta['redirect_reasons'][-1] == 302:

            try:
                redirected_product_id = re.findall("app/(.+?)/", response.meta['redirect_urls'][-1])[0]
            except:  # noqa E722
                raise Exception(f'Unable to read redirect url! For the following response meta: {response.meta}')
            self.db.add_review_scraped(redirected_product_id, redirected=True)
            self.db.commit()
            return

        # Load all reviews on current page.
        reviews = response.css('div .apphub_Card')
        for i, review in enumerate(reviews):
            load_rev = load_review(review, product_id, page, i, response)
            yield load_rev

        self.db.commit()
        # Navigate to next page.
        form = response.xpath('//form[contains(@id, "MoreContentForm")]')
        if form:
            yield self.process_pagination_form(form, page, product_id)
        else:
            self.db.add_review_scraped(product_id)
            self.db.commit()

    def form_request_from_last_url(self, url):
        if '?' not in url:
            return

        form_data = dict(urllib.parse.parse_qsl(url.split('?')[1]))
        action = f'https://steamcommunity.com/app/{form_data["appid"]}/homecontent/'
        meta = {'prev_page': str(int(form_data['p']) - 1), 'product_id': form_data["appid"]}

        form_request = FormRequest(
            url=action,
            method='GET',
            formdata=form_data,
            callback=self.parse,
            meta=meta,
            dont_filter=True
        )
        return form_request

    def process_pagination_form(self, form, page=None, product_id=None):
        action = form.xpath('@action').extract_first()
        names = form.xpath('input/@name').extract()
        values = form.xpath('input/@value').extract()

        formdata = dict(zip(names, values))
        meta = dict(prev_page=page, product_id=product_id)

        return FormRequest(
            url=action,
            method='GET',
            formdata=formdata,
            callback=self.parse,
            meta=meta,
            dont_filter=True
        )

    def closed(self, reason):
        self.db.close()
