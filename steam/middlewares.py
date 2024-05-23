import logging
import os
from w3lib.url import url_query_cleaner

from scrapy.dupefilters import RFPDupeFilter
from scrapy.extensions.httpcache import FilesystemCacheStorage
from scrapy.utils.request import request_fingerprint

logger = logging.getLogger(__name__)


def strip_snr(request):
    """Remove snr query query from request.url and return the modified request."""
    url = url_query_cleaner(request.url, ['snr'], remove=True)
    return request.replace(url=url)


class SteamCacheStorage(FilesystemCacheStorage):
    def _get_request_path(self, spider, request):
        request = strip_snr(request)
        key = request_fingerprint(request)
        return os.path.join(self.cachedir, spider.name, key[0:2], key)


class SteamDupeFilter(RFPDupeFilter):
    def request_fingerprint(self, request):
        request = strip_snr(request)
        return super().request_fingerprint(request)
        

class AddAgeCheckCookieMiddleware(object):
    @staticmethod
    def process_request(request, spider):
        if spider.name == 'products' and not request.cookies:
            request.cookies = {"wants_mature_content": "1", "lastagecheckage": "1-0-1985", "birthtime": '470703601'}

