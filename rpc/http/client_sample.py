from rpc.http import JsonRPCClient


class MyClient(JsonRPCClient):
    def echo(self, msg):
        resp = self.call('echo', **{'msg': msg})
        return resp

    def get_time(self):
        resp = self.call('get_time')
        return resp['now']


def main():
    client = MyClient("http://localhost:8008/my_rpc_path")
    print(client.echo("Hello world"))
    print(client.get_time())


if __name__ == '__main__':
    main()
