import random
import logging
import urllib.parse

import selenium.webdriver
import selenium.common.exceptions

from typing import Dict, List
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

from manager.launchers import BaseLauncher
from persistence.model.task import LoginInfo
from utils import load_class_type


class LoginInfoManager:
    def __init__(self, login_data: List[dict]):
        self.login_data_list = login_data
        self.login_info_dict = dict()

    @classmethod
    def create_driver(cls, login_url: str) -> WebDriver:
       #try:
       #    driver = cls.create_firefox_driver()
       #except (FileNotFoundError, selenium.common.exceptions.WebDriverException):
       #    logging.warning("[create_driver] firefox not found. using chrome")
       #    driver = cls.create_chrome_driver()
        driver = cls.create_chrome_driver()

        driver.get(login_url)
        return driver

    @classmethod
    def create_firefox_driver(cls) -> WebDriver:
        options = selenium.webdriver.FirefoxOptions()
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        return selenium.webdriver.Firefox(options=options)

    @classmethod
    def create_chrome_driver(cls) -> WebDriver:
        caps = DesiredCapabilities.CHROME
        caps['loggingPrefs'] = {'performance': 'ALL'}
        options = selenium.webdriver.ChromeOptions()
        # TODO 貌似Headless模式被icourse163反爬了，headless加载不上JS渲染的页面
        prefs = {"profile.managed_default_content_settings.images": 2}
        options.add_experimental_option("prefs", prefs)
        options.add_argument('headless')
        options.add_argument('disable-gpu')
        options.add_experimental_option('w3c', False)
        driver = selenium.webdriver.Chrome(options=options, desired_capabilities=caps)
        return driver
    def do_login(self) -> Dict[str, List[LoginInfo]]:
        for data in self.login_data_list:
            domain = data['domain']
            module = data['launcher']
            launcher_type = load_class_type(module)
            if not issubclass(launcher_type, BaseLauncher):
                logging.error("[LoginInfoManager.do_login] class [%s] is not a launcher.", launcher_type)
                raise TypeError("class [%s] is not a launcher." % module)

            url = data['login_url']
            login_infos = data['login_infos']

            self.login_info_dict[domain] = list()
            for info in login_infos:
                driver = self.create_driver(url)
                launcher = launcher_type(driver)
                res = launcher.login(**info)
                logging.info('[do_login] [%s] login res: [%s]', domain, res.__dict__)
                self.login_info_dict[domain].append(res)
                driver.quit()
        logging.info("[do_login] finished. num: %s", len(self.login_info_dict))
        return self.login_info_dict

    def do_login_domain(self, url: str, login_info: dict) -> LoginInfo.__dict__:
        url_struct = urllib.parse.urlparse(url)
        domain = url_struct.netloc
        old_info = LoginInfo()
        old_info.login_data = login_info["login_data"]
        old_info.cookies = login_info["cookies"]
        old_info.session = login_info["session"]
        old_info.proxy = login_info["proxy"]
        old_info.user_agent = login_info["user_agent"]
        for info in self.login_info_dict[domain]:
            if old_info.cookies[0]["value"] == info.cookies[0]["value"] and old_info.cookies[0]["name"] == info.cookies[0]["name"]:
                logging.info("[do_login_domain] Logininfo exists, login is expired domain: %s .", domain)
                # if info == old_info:
                self.login_info_dict[domain].remove(info)
                for data in self.login_data_list:
                    if domain == data['domain']:
                        module = data['launcher']
                        launcher_type = load_class_type(module)
                        if not issubclass(launcher_type, BaseLauncher):
                            logging.error("[LoginInfoManager.do_login_domain] class [%s] is not a launcher.",
                                          launcher_type)
                            raise TypeError("class [%s] is not a launcher." % module)
                        url = data['login_url']
                        login_infos = data['login_infos']
                        for logininfo in login_infos:
                            if logininfo["username"] == old_info.login_data["username"]:
                                driver = self.create_driver(url)
                                launcher = launcher_type(driver)
                                res = launcher.login(**logininfo)
                                logging.info('[do_login_domain] [%s] login res: [%s]', domain, res.__dict__)
                                self.login_info_dict[domain].append(res)
                                driver.quit()
                                print(self.login_info_dict[domain][0].__dict__)
                                logging.info("[do_login_domain] finished login domain: %s, sent login_info to crawler",
                                             domain)
                                return res
        logging.info("[do_login_domain] Logininfo doesn't exists, return latest logininfo domain: %s .", domain)
        new_info = self.get_login_info_rand(url)
        logging.info("Sending new login info:%s", new_info.__dict__)
        return new_info

    def get_all_login_info(self) -> Dict[str, List[LoginInfo]]:
        return self.login_info_dict

    def get_login_info_list(self, url) -> List[LoginInfo]:
        url_struct = urllib.parse.urlparse(url)
        domain = url_struct.netloc

        if domain not in self.login_info_dict.keys():
            logging.warning("[LoginInfoManager.get_login_info_list] No login info of domain [%s], url: [%s]", domain, url)
            # raise KeyError("no login info of domain [%s]", domain)
            return None
        return self.login_info_dict[domain]

    def get_login_info_rand(self, url: str) -> LoginInfo:
        info_list = self.get_login_info_list(url)
        if not info_list:
            return None
        index = random.randrange(0, len(info_list))
        return info_list[index]
