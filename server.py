import tornado.ioloop
import tornado.web
from tornado.options import define, options, parse_command_line
from data_loading import load_stops

from fetch_cta_data import fetch_cta_data
from get_nearest_stop import get_nearest_stop


define("port", default=8888, help="run on the given port", type=int)


class DefaultHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self):
        self.set_header("Content-Type", "text/plain")
        self.write("Success!")
        self.finish()


class NearestStopHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self, my_lat, my_lon):
        self.set_header("Content-Type", "text/plain")
        stops = load_stops()
        stop = get_nearest_stop(stops, [float(my_lat), float(my_lon)])
        self.write(stop.to_string())
        self.finish()


application = tornado.web.Application([
    (r'/', DefaultHandler),
    (r'/stops/(-?[0-9]+\.*[0-9]*)/(-?[0-9]+\.*[0-9]*)', NearestStopHandler),
])

if __name__ == '__main__':
    fetch_cta_data()

    parse_command_line()
    application.listen(options.port)
    tornado.ioloop.IOLoop.instance().start()