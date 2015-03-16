import pandas as pd
from keys import bus_key, train_key
import datetime
import json
import urllib
import tornado.httpclient
import tornado.web
import tornado.escape
import StringIO
from tornado.httputil import url_concat

import xml.etree.ElementTree as etree

#root = etree.parse(file_name)

BUS_URL = 'http://www.ctabustracker.com/bustime/api/v1/'
TRAIN_URL = 'http://lapi.transitchicago.com/api/1.0/'

class CtaApiManager():
    def __init__(self):
        print "init CtaApiManager"
        self.bus_params = BUS_PARAMS
        self.bus_url = BUS_URL

    def get_bus_routes(self):
        print "getting bus routes"

        http_client = tornado.httpclient.HTTPClient(defaults=self.bus_params)
        r = http_client.fetch(self.bus_url)
        print r.body


class BusAPIHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self, slug=None):
        params = {'key': bus_key}
        fields = ['vid', 'rt', 'rtdir', 'dir', 'pid', 'stpid', 'top']
        for f in fields:
            arg = self.get_argument(f, default=None)
            if arg:
                params[f] = arg

        http = tornado.httpclient.AsyncHTTPClient()
        url = url_concat(BUS_URL+slug, params)
        print url
        http.fetch(url, callback=self.on_response)


    def on_response(self, response):
        if response.error:
            raise tornado.web.HTTPError(500)
        self.write(convert_xml_to_json(response.body))
        self.finish()

class TrainAPIHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self, slug=None):
        params = {'key': train_key}
        fields = ['mapid', 'rt', 'runnumber', 'stpid', 'max']
        for f in fields:
            arg = self.get_argument(f, default=None)
            if arg:
                params[f] = arg

        http = tornado.httpclient.AsyncHTTPClient()
        url = url_concat(TRAIN_URL+'tt'+slug+'.aspx', params)
        print url
        http.fetch(url, callback=self.on_response)


    def on_response(self, response):
        if response.error:
            raise tornado.web.HTTPError(500)
        self.write(convert_xml_to_json(response.body))
        self.finish()

def convert_xml_to_json(xml_string):
    root = etree.fromstring(xml_string)
    d = []
    for x in root:
        item = {}
        if len(x) > 0:
            for y in x:
                item[y.tag] = y.text
        else:
            item[x.tag] = x.text
        d.append(item)
    return json.dumps(d)

if __name__ == '__main__':
    mgr = CtaApiManager()
    mgr.get_bus_routes()