import logging

from rpc.http import JsonRPCClient


class PipelineClient(JsonRPCClient):
    def put_data(self, pipeline_module: str, pipeline_args: dict, schedule_task_id: str, task_id: str, data):
        logging.info("[put_data] sending. task_id: %s, data: %s", task_id, data)
        return self.call('put_data', pipeline_module=pipeline_module, pipeline_args=pipeline_args,
                         schedule_task_id=schedule_task_id, task_id=task_id, data=data)
