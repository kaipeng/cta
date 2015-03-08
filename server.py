import tornado.ioloop
import tornado.web
from tornado.options import define, options, parse_command_line

from data_loading import load_stops, load_calendar, load_stop_times, load_trips

from fetch_cta_data import fetch_cta_data
from get_nearest_stop import get_nearest_stop


NUMBER = r'-?[0-9]+\.*[0-9]*'

define("port", default=8888, help="run on the given port", type=int)

class DataManager():
    def __init__(self):
        fetch_cta_data()
        print "Parsing Data into memory"
        self.stops = load_stops()
        self.calendar = load_calendar()
        self.stop_times = load_stop_times()
        self.trips = load_trips()
        print "Finished Parsing"

class DefaultHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self):
        self.set_header("Content-Type", "text/plain")
        self.write("Success!")
        self.finish()


class NearestStopHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self, my_lat, my_lon):
        self.set_header("Content-Type", "text/json")
        stops = data_manager.stops
        stop = get_nearest_stop(stops, [float(my_lat), float(my_lon)])
        self.write(stop.to_json(orient='records'))
        self.finish()

settings = {'debug': True}
application = tornado.web.Application([
    (r'/', DefaultHandler),
    ((r'/stops/(%s)/(%s)' % (NUMBER, NUMBER)), NearestStopHandler),
], **settings)

data_manager = None

if __name__ == '__main__':
    parse_command_line()
    data_manager = DataManager()

    application.listen(options.port)
    tornado.ioloop.IOLoop.instance().start()