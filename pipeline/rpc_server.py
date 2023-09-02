import logging
import threading

import utils

from pipeline.handler import PipelineHandler
from pipeline.pipelines import BasePipeline
from rpc.http import JsonRPCServer
from persistence.model.pipeline_config import PipelineConfig
from persistence.db.task_info_repo import TaskInfoRepository
from persistence.db.schedule_task_repo import ScheduleTaskInfoRepository
from rpc.http.server import JsonRPCThreadServer


class PipelineRPCServer(JsonRPCThreadServer):
    def __init__(self, config: PipelineConfig):
        super().__init__(config.bind_addr, config.bind_port, config.rpc_path, PipelineHandler)

        self.__pipelines = dict()
        self.config = config
        self.lock = threading.Lock()

    def get_pipeline(self, module_name: str, init_args: dict) -> BasePipeline:
        """
        Get pipeline. So each type pipeline with init_args only have one instance.
        :param module_name: pipeline module name
        :param init_args: init args
        :return: pipeline instance
        """
        self.lock.acquire()
        # if module_name not in self.__pipelines.keys():
        pipeline_type = utils.load_class_type(module_name)
        if not issubclass(pipeline_type, BasePipeline):
            raise TypeError("Pipeline module [%s] is not a Pipeline.", module_name)
        pipeline = pipeline_type(self.config)
        self.__pipelines[module_name] = pipeline
        pipeline_model = self.__pipelines[module_name]
        self.lock.release()
        return pipeline_model


if __name__ == '__main__':
    kConfigPath = '../config/pipeline.json'
    from persistence.config.json_config import JsonPipelineConfig
    conf = JsonPipelineConfig().read(kConfigPath)
    server = PipelineRPCServer(conf)
    server.serve_forever()
