import json
import logging

from persistence.model.report_config import ReportConfig
from .config import Config
from persistence.model.platform_config import PlatformConfig
from persistence.model.crawler_config import CrawlerConfig
from persistence.model.manager_config import ManagerConfig
from persistence.model.pipeline_config import PipelineConfig


class JsonConfig(Config):
    @classmethod
    def read(cls, filename: str) -> dict:
        with open(filename, 'r', encoding='utf-8') as fp:
            raw_data = fp.read()

        try:
            data_dict = json.loads(raw_data, encoding='utf-8')
        except ValueError as e:
            logging.error("[JsonConfig.read] file [%s] is not a legal json", filename)
            raise e

        if not isinstance(data_dict, dict):
            logging.error("[JsonConfig.read] file [%s] is not a dict", filename)
            raise ValueError("config file is not a dict")

        return data_dict

    @classmethod
    def write(cls, filename: str, config: dict):
        raise TypeError("Config write not impl")


class JsonPlatformConfig(JsonConfig):
    @classmethod
    def read(cls, filename: str) -> PlatformConfig:
        data_dict = super().read(filename)

        if not isinstance(data_dict.get('name'), str):
            logging.error("[JsonPlatformConfig.read] platform name [%s] illegal, should be a str",
                          data_dict.get('name'))
            raise ValueError("name [%s] illegal" % data_dict.get('name'))
        if not isinstance(data_dict.get('domain'), str):
            logging.error("[JsonPlatformConfig.read] platform domain [%s] illegal, should be a str",
                          data_dict.get('domain'))
            raise ValueError("domain [%s] illegal" % data_dict.get('domain'))

        config = PlatformConfig()
        config.__dict__.update(data_dict)

        return config


class JsonCrawlerConfig(JsonConfig):
    @classmethod
    def read(cls, filename: str) -> CrawlerConfig:
        data_dict = super().read(filename)

        if not isinstance(data_dict.get('manager_rpc_url'), str):
            logging.error("[JsonCrawlerConfig.read] manager_rpc_url [%s] illegal, should be a str",
                          data_dict.get('manager_rpc_url'))
            raise ValueError("manager_rpc_url [%s] illegal" % data_dict.get('manager_rpc_url'))

        config = CrawlerConfig()
        config.__dict__.update(data_dict)

        return config


class JsonManagerConfig(                                                           ):
    @classmethod
    def read(cls, filename: str) -> ManagerConfig:
        data_dict = super().read(filename)

        if not isinstance(data_dict.get('bind_addr'), str):
            logging.error("[JsonManagerConfig.read] bind_addr [%s] illegal, should be a str",
                          data_dict.get('bind_addr'))
            raise ValueError("bind_addr [%s] illegal" % data_dict.get('bind_addr'))
        if not isinstance(data_dict.get('bind_port'), int):
            logging.error("[JsonManagerConfig.read] bind_port [%s] illegal, should be a int",
                          data_dict.get('bind_port'))
            raise ValueError("bind_port [%s] illegal" % data_dict.get('bind_port'))

        config = ManagerConfig()
        config.__dict__.update(data_dict)

        return config


class JsonPipelineConfig(JsonConfig):
    @classmethod
    def read(cls, filename: str) -> PipelineConfig:
        data_dict = super().read(filename)

        if not isinstance(data_dict.get('bind_addr'), str):
            logging.error("[JsonPipelineConfig.read] bind_addr [%s] illegal, should be a str",
                          data_dict.get('bind_addr'))
            raise ValueError("bind_addr [%s] illegal" % data_dict.get('bind_addr'))
        if not isinstance(data_dict.get('bind_port'), int):
            logging.error("[JsonPipelineConfig.read] bind_port [%s] illegal, should be a int",
                          data_dict.get('bind_port'))
            raise ValueError("bind_port [%s] illegal" % data_dict.get('bind_port'))

        config = PipelineConfig()
        config.__dict__.update(data_dict)

        return config


class JsonReportConfig(JsonConfig):
    @classmethod
    def read(cls, filename: str) -> ReportConfig:
        data_dict = super().read(filename)
        config = ReportConfig()
        config.__dict__.update(data_dict)
        return config
