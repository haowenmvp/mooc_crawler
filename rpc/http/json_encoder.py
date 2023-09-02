import json
import datetime


class RPCJsonEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime.datetime):
            return o.strftime("%Y-%m-%d %H:%M:%S")
        else:
            return super().default(o)


def datetime_parser(d: dict):
    for k, v in d.items():
        if isinstance(v, str):
            try:
                d[k] = datetime.datetime.strptime(v, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                pass
    return d
