# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import datetime as dt


class SQLitePipeline:
    @staticmethod
    def process_item(item, spider):
        if spider.name == 'products':
            spider.db.add_product(item)

        if spider.name == 'reviews':
            spider.db.add_review(item, str(dt.datetime.today()), spider.review_ids, spider.rscrape_ids, spider.user_ids)

        return item
