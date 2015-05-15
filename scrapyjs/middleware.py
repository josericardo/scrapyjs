# -*- coding: utf-8 -*-
from __future__ import absolute_import
import json
import logging
from urlparse import urljoin, urlparse
from scrapy.exceptions import NotConfigured

from scrapy import log
from scrapy.http.headers import Headers

from w3lib.http import basic_auth_header

class SlotPolicy(object):
    PER_DOMAIN = 'per_domain'
    SINGLE_SLOT = 'single_slot'
    SCRAPY_DEFAULT = 'scrapy_default'

    _known = {PER_DOMAIN, SINGLE_SLOT, SCRAPY_DEFAULT}


class SplashMiddleware(object):
    """
    Scrapy downloader middleware that passes requests through Splash
    when 'splash' Request.meta key is set.
    """
    default_splash_url = 'http://127.0.0.1:8050'
    default_endpoint = "render.json"
    splash_extra_timeout = 5.0
    default_policy = SlotPolicy.PER_DOMAIN

    def __init__(self, crawler, splash_base_url, slot_policy):
        self.crawler = crawler
        self.splash_base_url = splash_base_url
        self.slot_policy = slot_policy
        self.splash_auth = None
        user = crawler.settings.get('SPLASH_USER')
        passwd = crawler.settings.get('SPLASH_PASS', '')
        if user:
            self.splash_auth = basic_auth_header(user, passwd)

    @classmethod
    def from_crawler(cls, crawler):
        splash_base_url = crawler.settings.get('SPLASH_URL', cls.default_splash_url)
        slot_policy = crawler.settings.get('SPLASH_SLOT_POLICY', cls.default_policy)

        if slot_policy not in SlotPolicy._known:
            raise NotConfigured("Incorrect slot policy: %r" % slot_policy)

        return cls(crawler, splash_base_url, slot_policy)

    def process_request(self, request, spider):
        splash_options = request.meta.get('splash')
        if not splash_options:
            return

        if request.method != 'GET':
            log.msg("Currently only GET requests are supported by SplashMiddleware; %s "
                    "will be handled without Splash" % request, logging.WARNING)
            return request

        meta = request.meta
        del meta['splash']
        meta['_splash_processed'] = splash_options

        slot_policy = splash_options.get('slot_policy', self.slot_policy)
        self._set_download_slot(request, meta, slot_policy)

        args = splash_options.setdefault('args', {})
        args.setdefault('url', request.url)

        if meta.get('proxy'):
            self.setup_variables_for_proxy_usage(args, meta, splash_options, request.headers)

        body = json.dumps(args, ensure_ascii=False)

        if 'timeout' in args:
            # User requested a Splash timeout explicitly.
            #
            # We can't catch a case when user requested `download_timeout`
            # explicitly because a default value for `download_timeout`
            # is set by DownloadTimeoutMiddleware.
            #
            # As user requested Splash timeout explicitly, we shouldn't change
            # it. Another reason not to change the requested Splash timeout is
            # because it may cause a validation error on the remote end.
            #
            # But we can change Scrapy `download_timeout`: increase
            # it when it's too small. Decreasing `download_timeout` is not
            # safe.
            timeout_current = meta.get('download_timeout', 1e6)  # no timeout means infinite timeout
            timeout_expected = float(args['timeout']) + self.splash_extra_timeout

            if timeout_expected > timeout_current:
                meta['download_timeout'] = timeout_expected

        endpoint = splash_options.setdefault('endpoint', self.default_endpoint)
        splash_base_url = splash_options.get('splash_url', self.splash_base_url)
        splash_url = urljoin(splash_base_url, endpoint)

        splash_meta = meta.copy()
        splash_meta.update(splash_options.get('meta', {}))

        req_rep = request.replace(
            url=splash_url,
            method='POST',
            body=body,
            meta=splash_meta,
            # FIXME: original HTTP headers (including cookies)
            # are not respected.
            headers=Headers({'Content-Type': 'application/json'}),
        )

        if self.splash_auth:
            req_rep.headers['Authorization'] = self.splash_auth

        self.crawler.stats.inc_value('splash/%s/request_count' % endpoint)
        return req_rep

    def setup_variables_for_proxy_usage(self, args, meta, splash_options, headers):
        """Makes the proxy usage via Splash transparent

        Uses the proxy defined in the request's meta attribute to build a request
        using a Splash lua script.
        It also forwards the request's headers to the proxy, as some proxies
        require some headers to be defined).
        """
        def headers_as_lua_list():
            splash_headers = ['["%s"] = "%s"' % (k, v[0])
                              for k,v in headers.iteritems()]

            return ",\n".join(splash_headers)

        def baseurl_as_lua_str():
            if args.get('baseurl'):
                return '"%s"' % args['baseurl']
            return 'nil'

        parsed_proxy_url = urlparse(meta['proxy'])
        host = parsed_proxy_url.hostname
        port = parsed_proxy_url.port

        script = """
        function main(splash)
            splash:on_request(function(request)
                request:set_proxy{
                    host = "%s",
                    port = %s,
                }
            end)

            splash:set_custom_headers({ %s })

            assert(splash:go{splash.args.url, baseurl=%s})
            return splash:html()
        end
        """

        args['lua_source'] = script % (host, port, headers_as_lua_list(), baseurl_as_lua_str())
        splash_options['endpoint'] = 'execute'

        # prevents sending requests to Splash via the proxy
        del(meta['proxy'])

    def process_response(self, request, response, spider):
        splash_options = request.meta.get("_splash_processed")
        if splash_options:
            endpoint = splash_options['endpoint']
            self.crawler.stats.inc_value(
                'splash/%s/response_count/%s' % (endpoint, response.status)
            )

        return response

    def _set_download_slot(self, request, meta, slot_policy):
        if slot_policy == SlotPolicy.PER_DOMAIN:
            # Use the same download slot to (sort of) respect download
            # delays and concurrency options.
            meta['download_slot'] = self._get_slot_key(request)

        elif slot_policy == SlotPolicy.SINGLE_SLOT:
            # Use a single slot for all Splash requests
            meta['download_slot'] = '__splash__'

        elif slot_policy == SlotPolicy.SCRAPY_DEFAULT:
            # Use standard Scrapy concurrency setup
            pass

    def _get_slot_key(self, request_or_response):
        return self.crawler.engine.downloader._get_slot_key(request_or_response, None)
