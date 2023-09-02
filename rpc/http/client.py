import json
import datetime
import requests

from .json_encoder import RPCJsonEncoder, datetime_parser


class JsonRPCClient:
    def __init__(self, rpc_url: str):
        self.url = rpc_url

    def call(self, method: str, **kwargs):
        req = json.dumps({'method': method, 'param': kwargs}, cls=RPCJsonEncoder)

        resp_http = requests.post(self.url, data=req)
        if resp_http.status_code != 200:
            raise TypeError(resp_http.reason)
        resp = json.loads(resp_http.text, object_hook=datetime_parser)

        if not resp['success']:
            raise TypeError("RPC call failed. msg: %s" % resp['msg'])

        return resp['data']
