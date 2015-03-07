import tornado.ioloop
import tornado.web
from tornado.options import define, options, parse_command_line

define("port", default=8888, help="run on the given port", type=int)


class DefaultHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self):
        self.set_header("Content-Type", "text/plain")
        self.write("Success!")
        self.finish()


application = tornado.web.Application([
    (r'/', DefaultHandler),
])

if __name__ == '__main__':
    parse_command_line()
    application.listen(options.port)
    tornado.ioloop.IOLoop.instance().start()