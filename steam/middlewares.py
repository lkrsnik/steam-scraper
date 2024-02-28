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

import binascii
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


class CircumventAgeCheckMiddleware(RedirectMiddleware):
    def _redirect(self, redirected, request, spider, reason):
        # Only overrule the default redirect behavior
        # in the case of mature content checkpoints.
        # if re.findall('agecheck/app/(.*)', redirected.url):
        #     print('here!')

        # if not re.findall('agecheck/app/(.*)', redirected.url):
        #     return super()._redirect(redirected, request, spider, reason)
        if not re.findall('app/(.*)/agecheck', redirected.url):
            return super()._redirect(redirected, request, spider, reason)

        logger.debug(f'Button-type age check triggered for {request.url}.')

        # sa = Request(url=request.url,
        #                cookies={'wants_mature_content': '1'},
        #                meta={'dont_cache': True},
        #                callback=spider.parse_product)
        #
        # a = process_request(sa, spider)

        # a = Request(url=request.url,
        #                cookies={'wants_mature_content': '1'},
        #                meta={'dont_cache': True},
        #                callback=spider.parse_product)
        #
        # inspect_response(a, spider)

        # am_sh = Shell(spider)
        # am = am_sh.fetch(a)
        # resp = fetch(a)

        return Request(url=request.url,
                       cookies={"wants_mature_content": "1", "lastagecheckage": "1-0-1985", "birthtime": '470703601'},
                       meta={'dont_cache': True},
                       callback=spider.parse_product)
