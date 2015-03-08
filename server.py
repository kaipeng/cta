from threading import Timer

import tornado.ioloop
import tornado.web
from tornado.options import define, options, parse_command_line

from data_loading import load_stops, load_calendar, load_stop_times, load_trips
from data_loading import load_trips_date, load_stop_times_date, load_calendar_date

from fetch_cta_data import fetch_cta_data
from get_nearest_stop import get_nearest_stop

import datetime
import time
import pandas as pd

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
        self.stop_times_window = None
        print "Finished Parsing"

    def load_stop_times_in_window(self, start_datetime, window_in_hours):
        start = time.time()
        calendar_today = load_calendar_date(self.calendar, start_datetime, days_delta=0)
        trips_today = load_trips_date(self.trips, calendar_today)
        stop_times_today = load_stop_times_date(self.stop_times, trips_today)
        start_time_string = start_datetime.strftime('%H:%M:%S')
        end_datetime = start_datetime + datetime.timedelta(hours=window_in_hours)
        end_time_string = end_datetime.strftime('%H:%M:%S')

        # case where window crosses into next day (maybe new schedule)
        if start_datetime.date() < end_datetime.date():
            stop_times_window = stop_times_today[(stop_times_today.arrival_time > start_time_string)]

            calendar_tomorrow = load_calendar_date(self.calendar, start_datetime, days_delta=0)
            trips_tomorrow = load_trips_date(self.trips, calendar_tomorrow)
            stop_times_tomorrow = load_stop_times_date(self.stop_times, trips_tomorrow)
            stop_times_window_tomorrow = stop_times_tomorrow[(stop_times_tomorrow.arrival_time < end_time_string)]

            stop_times_window_tomorrow['arrival_time_effective'] = stop_times_window_tomorrow['arrival_time_24']
            stop_times_window_tomorrow['departure_time_effective'] = stop_times_window_tomorrow['departure_time_24']
            stop_times_window['arrival_time_effective'] = stop_times_window['arrival_time']
            stop_times_window['departure_time_effective'] = stop_times_window['departure_time']

            stop_times_window = pd.concat(stop_times_window, stop_times_window_tomorrow)

        else:
            stop_times_window = stop_times_today[(stop_times_today.arrival_time > start_time_string)
                                                 & (stop_times_today.arrival_time < end_time_string)]
            stop_times_window['arrival_time_effective'] = stop_times_window['arrival_time']
            stop_times_window['departure_time_effective'] = stop_times_window['departure_time']

        end = time.time()
        print end - start
        self.stop_times_window = stop_times_window
        return stop_times_window


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


def schedule_data_manager_job(delay_in_seconds, job_method):
    Timer(delay_in_seconds, job_method, [delay_in_seconds]).start()


def update_data_manager_job(delay_in_seconds):
    print "updating data manager"
    update_data_manager()
    schedule_data_manager_job(delay_in_seconds, update_data_manager_job)


def update_data_manager():
    new_data_manager = DataManager()
    global data_manager
    data_manager = new_data_manager

def update_stop_times_window_job(delay_in_seconds):
    print "updating stop times window"
    global data_manager
    data_manager.load_stop_times_in_window(datetime.datetime.now(), 3)
    schedule_data_manager_job(delay_in_seconds, update_stop_times_window_job)

settings = {'debug': True}
application = tornado.web.Application([
    (r'/', DefaultHandler),
    ((r'/stops/(%s)/(%s)' % (NUMBER, NUMBER)), NearestStopHandler),
], **settings)

data_manager = None

if __name__ == '__main__':
    parse_command_line()
    data_manager = DataManager()

    schedule_data_manager_job(60 * 60 * 6, update_data_manager_job)
    schedule_data_manager_job(60 * 60 * 1, update_stop_times_window_job)

    application.listen(options.port)
    tornado.ioloop.IOLoop.instance().start()
