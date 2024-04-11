import logging
import re
from scrapy.http import Request

from w3lib.url import canonicalize_url, url_query_cleaner

from scrapy.http import FormRequest
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule

from ..items import ProductItem, ProductItemLoader
from ..sqlite import Database

logger = logging.getLogger(__name__)


def load_product(response):
    """Load a ProductItem from the product page response."""
    loader = ProductItemLoader(item=ProductItem(), response=response)

    url = url_query_cleaner(response.url, ['snr'], remove=True)
    url = canonicalize_url(url)
    loader.add_value('url', url)

    found_id = re.findall('/app/(.*?)/', response.url)
    if found_id:
        id = found_id[0]
        reviews_url = f'http://steamcommunity.com/app/{id}/reviews/?browsefilter=mostrecent&p=1'
        news_url = f'http://store.steampowered.com/news/app/{id}'
        loader.add_value('reviews_url', reviews_url)
        loader.add_value('news_url', news_url)
        loader.add_value('id', id)

    # Publication details.
    details = response.css('.details_block').extract_first()
    try:
        details = re.split(r'<br>|<div class="dev_row">|<\/div>', details)

        for line in details:
            line = re.sub('<[^<]+?>', '', line)  # Remove tags.
            line = re.sub('[\r\t\n]', '', line).strip()
            for prop, name in [
                ('Title:', 'title'),
                ('Genre:', 'genres'),
                ('Developer:', 'developer'),
                ('Publisher:', 'publisher'),
                ('Release Date:', 'release_date')
            ]:
                if prop in line:
                    item = line.replace(prop, '').strip()
                    loader.add_value(name, item)
    except:  # noqa E722
        pass

    description_about = response.css('#game_area_description').extract_first()
    try:
        text = ''

        line = re.sub('<[^<]+?>', '', description_about)  # Remove tags.
        line = re.sub('[\r\t\n]+', '\n', line).strip()
        text += line.strip() + '\n'
        loader.add_value('description_about', text)
    except:  # noqa E722
        pass

    description_reviews = response.css('#game_area_reviews').extract_first()
    try:
        text = ''

        line = re.sub('<[^<]+?>', '', description_reviews)  # Remove tags.
        line = re.sub('[\r\t\n]+', '\n', line).strip()
        text += line.strip() + '\n'
        loader.add_value('description_reviews', text)
    except:  # noqa E722
        pass

    loader.add_css('app_name', '.apphub_AppName ::text')
    loader.add_css('specs', 'a.game_area_details_specs_ctn ::text')
    loader.add_css('tags', 'a.app_tag::text')

    price = response.css('.game_purchase_price ::text').extract_first()
    if not price:
        price = response.css('.discount_original_price ::text').extract_first()
        loader.add_css('discount_price', '.discount_final_price ::text')
    loader.add_value('price', price)

    sentiment = response.css('.game_review_summary').xpath(
        '../*[@itemprop="description"]/text()').extract()
    loader.add_value('sentiment', sentiment)
    # loader.add_css('n_reviews', '.responsive_hidden', re='\(([\d,]+) reviews\)')
    loader.add_css('n_reviews', '.responsive_hidden', re='\(([\d,]+)\)')

    loader.add_xpath(
        'metascore',
        '//div[@id="game_area_metascore"]/div[contains(@class, "score")]/text()')

    early_access = response.css('.early_access_header')
    if early_access:
        loader.add_value('early_access', True)
    else:
        loader.add_value('early_access', False)

    return loader.load_item()


class ProductSpider(CrawlSpider):
    name = 'products'
    start_urls = ['http://store.steampowered.com/search/?sort_by=Released_DESC']

    allowed_domains = ['steampowered.com']

    rules = [
        Rule(LinkExtractor(
             allow='/app/(.+)/',
             restrict_css='#search_result_container'),
             callback='parse_product',
             process_links='process_app_links'),
        Rule(LinkExtractor(
             allow='page=(\d+)',
             restrict_css='.search_pagination_right'))
    ]

    def __init__(self, steam_id=None, sqlite_path=None, overwrite_db=False, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.db = Database(sqlite_path, overwrite_db == 'True')
        self.steam_id = steam_id
        self.processed_products = self.db.get_product_ids()


    def start_requests(self):
        if self.steam_id:
            yield Request(f'http://store.steampowered.com/app/{self.steam_id}/',
                          callback=self.parse_product)
        else:
            yield from super().start_requests()

    @staticmethod
    def get_product_id(response):
        product_id = response.meta.get('product_id', None)

        if not product_id:
            try:
                return re.findall("app/(.+?)/", response.url)[0]
            except Exception as e:
                print(e)
                try:
                    return re.findall("app/(.+?)$", response.url)[0]
                except:
                    return None
        else:
            return product_id

    @staticmethod
    def parse_product(response):
        yield load_product(response)

    def process_app_links(self, links):
        for link in links:
            app_id = re.findall('/app/(.*?)/', link.url)
            if len(app_id) == 1 and int(app_id[0]) in self.processed_products:
                continue
            yield link

    def _build_request(self, rule_index, link):
        if '815760' in link.url:
            print('here!')
        return Request(
            url=link.url,
            callback=self._callback,
            cookies={"wants_mature_content": "1", "lastagecheckage": "1-0-1985", "birthtime": '470703601'},
            errback=self._errback,
            meta=dict(rule=rule_index, link_text=link.text),
        )
