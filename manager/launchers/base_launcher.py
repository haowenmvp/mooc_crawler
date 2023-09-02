import abc

from selenium.webdriver.remote.webdriver import WebDriver
from persistence.model.task import LoginInfo


class BaseLauncher:
    kWaitSecond = 5

    def __init__(self, driver: WebDriver):
        self.driver = driver

    @abc.abstractmethod
    def login(self, username='', password='', qr_path='') -> LoginInfo:
        pass
