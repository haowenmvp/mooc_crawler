import json
import logging

from typing import Type

from .json_encoder import RPCJsonEncoder, datetime_parser
from http.server import HTTPServer, BaseHTTPRequestHandler, ThreadingHTTPServer


class JsonRPCHandler(BaseHTTPRequestHandler):
    def log_error(self, format, *args):
        logging.error(format, *args)

    def log_message(self, format, *args):
        logging.info(format, *args)

    def handle_json_request(self, req) -> dict:
        if not isinstance(req, dict) or 'method' not in req or 'param' not in req or not isinstance(req['param'], dict):
            return self.build_err('request format error')

        method_name = req['method'] + "_handler"
        logging.debug("[JsonRPCHandler] calling method [%s]", method_name)
        if not hasattr(self, method_name):
            return self.build_err('method [%s] not found' % req['method'])
        method = getattr(self, method_name)

        try:
            resp = method(**req['param'])
        except TypeError:
            return self.build_err("Parameters error")

        if not isinstance(resp, dict):
            raise ValueError("resp should be a dict")
        logging.debug("[JsonRPCHandler] called method [%s]", method_name)
        return resp

    @staticmethod
    def build_err(msg: str, *args) -> dict:
        return {'success': 0, 'msg': msg % args, 'data': ''}

    @staticmethod
    def build_res(data=None):
        return {'success': 1, 'msg': 'ok', 'data': data}

    def do_POST(self):
        assert isinstance(self.server, (JsonRPCServer, JsonRPCThreadServer))
        if self.path != self.server.rpc_path:
            self.send_error(404, "Not Found")
            return

        if 'Content-Length' not in self.headers:
            self.send_error(400, "Bad Request", "Content-Length not found")
            return
        try:
            body_len = int(self.headers['Content-Length'])
        except ValueError:
            self.send_error(400, "Bad Request", "Content-Length is not a number")
            return

        req_str = self.rfile.read(body_len).decode('utf-8')
        try:
            req = json.loads(req_str, encoding='utf-8', object_hook=datetime_parser)
        except ValueError:
            self.send_error(400, "Bad Request", "Request is not a json")
            return

        resp = self.handle_json_request(req)

        resp_str = json.dumps(resp, cls=RPCJsonEncoder)
        self.send_response(200, 'OK')
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(resp_str)))
        self.end_headers()
        self.wfile.write(resp_str.encode('utf-8'))
        return


class JsonRPCServer(HTTPServer):
    def __init__(self, bind_addr: str, port: int, rpc_path: str, handler_type: Type[JsonRPCHandler]):
        """
        Init RPC server
        :param bind_addr: Server listen addr
        :param port: Server port
        :param handler_type: Type of RPC method handler
        """
        if not issubclass(handler_type, JsonRPCHandler):
            raise ValueError("handler_type [%s] is not subclass of JsonRPCHandler", str(handler_type))

        super().__init__((bind_addr, port), handler_type)

        self.addr = bind_addr
        self.port = port
        self.rpc_path = rpc_path
        self.handler_type = handler_type

    def serve_forever(self, poll_interval=0.5):
        logging.info("[JsonRPCServer] start serve at %s: %s", self.addr, self.port)
        super().serve_forever(poll_interval)


class JsonRPCThreadServer(ThreadingHTTPServer):
    def __init__(self, bind_addr: str, port: int, rpc_path: str, handler_type: Type[JsonRPCHandler]):
        """
        Init RPC server
        :param bind_addr: Server listen addr
        :param port: Server port
        :param handler_type: Type of RPC method handler
        """
        if not issubclass(handler_type, JsonRPCHandler):
            raise ValueError("handler_type [%s] is not subclass of JsonRPCHandler", str(handler_type))

        super().__init__((bind_addr, port), handler_type)

        self.addr = bind_addr
        self.port = port
        self.rpc_path = rpc_path
        self.handler_type = handler_type

    def serve_forever(self, poll_interval=0.5):
        logging.info("[JsonRPCThreadServer] start serve at %s: %s", self.addr, self.port)
        super().serve_forever(poll_interval)
