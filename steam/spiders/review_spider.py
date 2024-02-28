import os
import re

import pandas as pd
import scrapy
from scrapy.http import FormRequest, Request
from w3lib.url import url_query_parameter

from ..items import ReviewItem, ReviewItemLoader, str_to_int


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
    if user_id:
        loader.add_value('user_id', user_id[0])
    else:
        #user_id = loader.get_css('.apphub_CardContentAuthorName a::attr(href)', re='.*/id/(.+)/')
        loader.add_css('user_id', '.apphub_CardContentAuthorName a::attr(href)', re='.*/id/(.+)/')
    username = loader.get_css('.apphub_CardContentAuthorName a::text')
    # loader.add_css('user_id', '.apphub_CardContentAuthorName a::attr(href)', re='.*/profiles/(.+)/')
    loader.add_css('username', '.apphub_CardContentAuthorName a::text')
    has_failed = False
    if not username or not text:
        has_failed = True
        print(f'Repeating {product_id}-p{page}!')
        print('---------------------------------------------------------------------')
        print(response.text)
        print('+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++')
        with open(f'review_fails/{product_id}-p{page}.html', 'w') as wf:
            wf.write(response.text)
        # raise ValueError('NO USERNAME OR TEXT!')
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

    return has_failed, loader.load_item()


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
        except:  # noqa E722
            return None
    else:
        return product_id


class ReviewSpider(scrapy.Spider):
    name = 'reviews'
    test_urls = [
        # Full Metal Furies
        'http://steamcommunity.com/app/416600/reviews/?browsefilter=mostrecent&p=1',
    ]

    def __init__(self, processed_products_path=None, already_processed_file=None, successfully_processed_file=None, unsuccessfully_processed_file=None, steam_id=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.processed_products_path = processed_products_path
        self.steam_id = steam_id
        self.failed_requests = {}
        self.already_processed = set()
        if already_processed_file:
            with open(already_processed_file, 'r') as f:
                self.already_processed = set(f.read().split())
        if successfully_processed_file and os.path.exists(successfully_processed_file):
            os.remove(successfully_processed_file)
        self.successfully_processed_file = successfully_processed_file
        if unsuccessfully_processed_file and os.path.exists(unsuccessfully_processed_file):
            os.remove(unsuccessfully_processed_file)
        self.unsuccessfully_processed_file = unsuccessfully_processed_file

    def read_urls(self):
        products_df = pd.read_csv(self.processed_products_path, index_col=0)
        # ignore products that have less than 10 reviews
        products_filtered_df = products_df[products_df['n_reviews'].notna()]
        for _, product in products_filtered_df.iterrows():
            url = product['reviews_url']
            if url:
                product_id = re.findall("app/(.+?)/", url)[0]
                if product_id in self.already_processed:
                    continue
                yield scrapy.Request(url, cookies={"wants_mature_content_apps": product_id}, callback=self.parse)

    def start_requests(self):
        if self.steam_id:
            url = (
                f'http://steamcommunity.com/app/{self.steam_id}/reviews/'
                '?browsefilter=mostrecent&p=1'
            )
            yield Request(url, callback=self.parse)
        elif self.processed_products_path:
            yield from self.read_urls()
        else:
            for url in self.test_urls:
                yield Request(url, callback=self.parse)

    def parse(self, response):
        page = get_page(response)
        product_id = get_product_id(response)

        # Load all reviews on current page.
        reviews = response.css('div .apphub_Card')
        if (product_id, page) in self.failed_requests:
            self.failed_requests[(product_id, page)]['has_failed'] = False
        for i, review in enumerate(reviews):
            has_failed, load_rev = load_review(review, product_id, page, i, response)
            if (product_id, page) in self.failed_requests:
                self.failed_requests[(product_id, page)]['has_failed'] = self.failed_requests[(product_id, page)]['has_failed'] or has_failed
            yield load_rev

        # Navigate to next page.
        form = response.xpath('//form[contains(@id, "MoreContentForm")]')
        if form:
            yield self.process_pagination_form(form, page, product_id)
        else:
            if self.successfully_processed_file:
                with open(self.successfully_processed_file, 'a') as af:
                    af.write(product_id + '\n')

    def process_pagination_form(self, form, page=None, product_id=None):
        # if previous page had fails try repeating 10 times
        if (product_id, page - 1) in self.failed_requests:
            if self.failed_requests[(product_id, page - 1)]['has_failed']:
                self.failed_requests[(product_id, page - 1)]['fails'] += 1
                if self.failed_requests[(product_id, page - 1)]['fails'] > 10:
                    if self.unsuccessfully_processed_file:
                        with open(self.unsuccessfully_processed_file, 'a') as af:
                            af.write(product_id + '\n')
                        raise ValueError(f'Failed to process {product_id}!')
                else:
                    return self.failed_requests[(product_id, page - 1)]['form_request']
            else:
                del(self.failed_requests[(product_id, page - 1)])


        action = form.xpath('@action').extract_first()
        names = form.xpath('input/@name').extract()
        values = form.xpath('input/@value').extract()

        formdata = dict(zip(names, values))
        meta = dict(prev_page=page, product_id=product_id)

        self.failed_requests[(product_id, page+1)] = {}
        self.failed_requests[(product_id, page+1)]['fails'] = 0
        self.failed_requests[(product_id, page+1)]['has_failed'] = False
        self.failed_requests[(product_id, page+1)]['form_request'] = FormRequest(
            url=action,
            method='GET',
            formdata=formdata,
            callback=self.parse,
            meta=meta,
            dont_filter=True
        )

        return self.failed_requests[product_id, page+1]['form_request']
