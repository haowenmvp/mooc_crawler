import abc

from typing import List, Tuple


class BaseApi:
    def __init__(self, host: str, port: int, username: str, password: str, database: str):
        self.__host = host
        self.__port = port
        self.__user = username
        self.__password = password
        self.__database = database

    @abc.abstractmethod
    def query(self, table_name: str, query_field_list: List[str], conditions: dict) -> List[Tuple]:
        pass

    @abc.abstractmethod
    def insert(self, table_name: str, data_list: List[dict]):
        pass

    @abc.abstractmethod
    def update(self, table_name: str, data_list: List[dict], conditions_list: List[dict]):
        pass

    @abc.abstractmethod
    def delete(self, table_name: str, conditions: dict):
        pass
