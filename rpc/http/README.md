## HTTP Json RPC格式

#### Request
```json
{
  "method": "MethodName",
  "param": {
    "param1": 1,
    "param2": "param2"
  }
}
```

#### Response
```json
{
  "success": 1,
  "msg": "ok",
  "data": "method res as a json type. can be str, int, dict or list "
}
```

## Handler实现方式
1. 继承`JsonRPCHandler`
2. 重写`kRPCPath`属性，即RPC调用的位置
3. 实现需要的`method`
    * `method`声明约定：方法名后缀`_handler`
    * sample:
    ```python
   from datetime import datetime
   from rpc.http import JsonRPCHandler
   
   class MyHandler(JsonRPCHandler):
       kRPCPath = '/my_json_rpc'
    
       def echo_handler(self, msg):
           return self.build_res(msg)

       def get_time_handler(self):
           return self.build_res({'now': datetime.now().strftime('%Y-%m-%d %H:%M:%S')})

    ```
   
## Client实现方式
1. 继承`JsonRPCClient`
2. 使用`self.call(method: str, **kwargs)`实现需要调用的方法
    * sample:
    ```python
   from rpc.http import JsonRPCClient

   class MyClient(JsonRPCClient):
       def echo(self, msg):
           resp = self.call('echo', **{'msg': msg})
           return resp

       def get_time(self):
           resp = self.call('get_time')
           return resp['now']
    ```
