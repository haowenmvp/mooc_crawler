from datetime import datetime
from rpc.http import JsonRPCHandler, JsonRPCServer


class MyHandler(JsonRPCHandler):
    def echo_handler(self, msg):
        return self.build_res(msg)

    def get_time_handler(self):
        return self.build_res({'now': datetime.now().strftime('%Y-%m-%d %H:%M:%S')})


def main():
    server = JsonRPCServer('localhost', 8008, '/my_rpc_path', MyHandler)
    server.serve_forever()


if __name__ == '__main__':
    main()
