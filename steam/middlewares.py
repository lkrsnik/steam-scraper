import logging
import os
import re
from w3lib.url import url_query_cleaner

from scrapy import Request
from scrapy.downloadermiddlewares.redirect import RedirectMiddleware
from scrapy.dupefilters import RFPDupeFilter
from scrapy.extensions.httpcache import FilesystemCacheStorage
from scrapy.utils.request import request_fingerprint, fingerprint
from scrapy.shell import inspect_response

logger = logging.getLogger(__name__)


def strip_snr(request):
    """Remove snr query query from request.url and return the modified request."""
    url = url_query_cleaner(request.url, ['snr'], remove=True)
    return request.replace(url=url)


class SteamCacheStorage(FilesystemCacheStorage):
    def _get_request_path(self, spider, request):
        request = strip_snr(request)
        key = fingerprint(request)
        key = bytes.hex(key)
        return os.path.join(self.cachedir, spider.name, key[0:2], key)


class SteamDupeFilter(RFPDupeFilter):
    def request_fingerprint(self, request):
        request = strip_snr(request)
        return super().request_fingerprint(request)
