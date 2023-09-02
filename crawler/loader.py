import logging
import importlib
import urllib.parse
import threading

from typing import Type, Tuple

import utils

from persistence.model.task import ScheduleTask
from persistence.model.platform_config import PlatformConfig

from .fetcher import BaseFetcher


class FetcherLoader:
    __instance = None
    __instance_lock = threading.Lock()

    def __init__(self):
        """
        Single instance mode. USE FetcherLoader.instance() INSTEAD of this FetcherLoader().
        """
        self.__platform_dict = dict()

    @classmethod
    def instance(cls):
        if not cls.__instance:
            with cls.__instance_lock:
                if not cls.__instance:
                    cls.__instance = FetcherLoader()
        return cls.__instance

    def add_platform(self, platform: PlatformConfig):
        self.__platform_dict[platform.domain] = platform

    def get_pipeline_module(self, task: ScheduleTask) -> dict:
        fetcher_cfg = self.__get_fetcher_cfg(task)
        return fetcher_cfg['pipeline']

    def load(self, task: ScheduleTask) -> Type[BaseFetcher]:
        """
        Get a crawler class type according task.
        :param task: Task info
        :return: Crawler type
        """
        try:
            class_path = self.__get_fetcher_cfg(task)['module']
        except KeyError as e:
            logging.error("[FetcherLoader.load] cannot found fetcher module config.")
            raise e

        # load fetch class type
        class_type = utils.load_class_type(class_path)
        if not issubclass(class_type, BaseFetcher):
            logging.error("[FetcherLoader.load] class [%s] is not a fetcher.", class_path)
            raise TypeError("class [%s] is not a fetcher." % class_path)

        return class_type

    def load_platform(self, task: ScheduleTask) -> str:
        platform_config, domain = self.__get_platform_cfg(task)

        # Get fetcher module according fetch_type from config
        if not platform_config.platform:
            logging.error("[FetcherLoader.load_platform] platform name does not exists in platform_config for [%s].",
                          domain)
            raise ValueError("platform name does not exists in platform_config for [%s]." % domain)

        return platform_config.platform


    def __get_platform_cfg(self, task: ScheduleTask) -> Tuple[PlatformConfig, str]:
        url_struct = urllib.parse.urlparse(task.url)
        domain = url_struct.netloc
        logging.info("[loader.__get_platform_cfg] domain is [%s]", domain)

        # Get target platform config
        if domain not in self.__platform_dict.keys():
            logging.error("[FetcherLoader.__get_platform_cfg] platform [%s] didn't be added to platform dict.",
                          domain)
            raise ValueError("Platform [%s] not in platform dict." % domain)

        return self.__platform_dict[domain], domain

    def __get_fetcher_cfg(self, task: ScheduleTask) -> dict:
        fetcher_type = task.fetcher_type
        platform_config, domain = self.__get_platform_cfg(task)

        # Get fetcher module according fetch_type from config
        if fetcher_type not in platform_config.fetchers.keys():
            logging.error("[FetcherLoader.__get_fetcher_cfg] fetcher [%s] is not supported in platform [%s].",
                          fetcher_type, domain)
            raise ValueError("fetcher [%s] is not supported in platform [%s]." % (fetcher_type, domain))

        return platform_config.fetchers[fetcher_type]

