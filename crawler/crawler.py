import time
import logging
import threading
import traceback
from retrying import retry
import urllib.parse

import selenium.webdriver
import selenium.common.exceptions

from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

from .loader import FetcherLoader
from manager.rpc_client import ManagerClient
from pipeline.rpc_client import PipelineClient

from crawler.fetcher import BaseFetcher
from persistence.model.crawler_config import CrawlerConfig
from persistence.model.task import ScheduleTask, LoginInfo
from exceptions.offlineError import OfflineError


class Crawler:
    def __init__(self, config: CrawlerConfig):
        self.mgr_rpc_client = ManagerClient(config.manager_rpc_url)
        self.pl_rpc_client = PipelineClient(config.pipeline_rpc_url)
        self.platform_config_list = list()
        self.loader = FetcherLoader.instance()

        self.is_connected_to_mgr = False
        self.register_info = None
        self.keep_alive_thread = CrawlerKeepAliveThread(self)

    def register(self):
        if not self.is_connected_to_mgr:
            logging.info('[Crawler.register] start register crawler.')
            try:
                self.register_info = self.mgr_rpc_client.register()
                self.get_platform_config_from_manager()
            except Exception as e:
                logging.warning('[Crawler.register] cannot register crawler. err: [%s]', e)
                logging.warning('[Crawler.register] %s', traceback.format_exc())
                return False

            self.is_connected_to_mgr = True
            logging.info('[Crawler.register] finish register crawler. client_id: %s', self.mgr_rpc_client.client_id)

            if not self.keep_alive_thread.is_alive():
                logging.info("[Crawler.register] starting keep-alive thread")
                self.keep_alive_thread.start()

        return True

    def keep_alive(self):
        try:
            self.mgr_rpc_client.send_keep_alive()
        except Exception as e:
            logging.error("[Crawler.keep_alive] Cannot send keep-alive. err: [%s]", e)
            logging.error('[Crawler.keep_alive] %s', traceback.format_exc())
            self.is_connected_to_mgr = False

    def get_platform_config_from_manager(self):
        self.platform_config_list = self.mgr_rpc_client.get_platform_config_list()
        for config in self.platform_config_list:
            self.loader.add_platform(config)

    @classmethod
    def create_driver(cls, task: ScheduleTask) -> WebDriver:
        # try:
        #     driver = cls.create_firefox_driver(task)
        # except (FileNotFoundError, selenium.common.exceptions.WebDriverException):
        #     logging.warning("[create_driver] firefox not found. using chrome")
        # else:
        #     driver = cls.create_firefox_driver(task)
        driver = cls.create_chrome_driver(task)
        driver.get(task.url)
        # if task.login_info:
        #     logging.info("[crawler.create_driver] " + str(task.login_info.__dict__))
        #     cls.login_recovery(driver, task.login_info)
        # else:
        #     logging.warning("[create_driver]. Login_info is NULL. Not do login_recovery")

        driver.refresh()
        # driver.refresh()
        return driver

    @classmethod
    def create_firefox_driver(cls, task: ScheduleTask) -> WebDriver:
        options = selenium.webdriver.FirefoxOptions()
        options.add_argument("--headless")
        options.set_preference('permissions.default.image', 2)
        options.add_argument("--disable-gpu")
        return selenium.webdriver.Firefox(options=options)

    @classmethod
    def create_chrome_driver(cls, task: ScheduleTask) -> WebDriver:
        caps = DesiredCapabilities.CHROME
        caps['loggingPrefs'] = {'performance': 'ALL'}
        options = selenium.webdriver.ChromeOptions()
        # TODO 貌似Headless模式被icourse163反爬了，headless加载不上JS渲染的页面
        prefs = {"profile.managed_default_content_settings.images": 2}
        options.add_experimental_option("prefs", prefs)
        options.add_argument('headless')
        options.add_argument('disable-gpu')
        options.add_experimental_option('w3c', False)
        # if task.login_info:
        #     if task.login_info.proxy:
        #         options.add_argument(f"proxy-server=t{task.login_info.proxy}")
        #     if task.login_info.user_agent:
        #         options.add_argument(f'user-agent={task.login_info.user_agent}')
        driver = selenium.webdriver.Chrome(options=options, desired_capabilities=caps)
        return driver

    @classmethod
    def login_recovery(cls, driver: WebDriver, login_info: LoginInfo):
        for cookie in login_info.cookies:
            if 'expiry' in cookie:
                del cookie['expiry']
            driver.add_cookie(cookie)
        for session_key, session_val in login_info.session:
            driver.execute_script('sessionStorage.setItem(%s, %s)' % (session_key, session_val))
        logging.info("[crawler.login_recovery] Login recovery succeeded.")

    def fetch_data(self, fetcher: BaseFetcher, task: ScheduleTask) -> dict:
        if task.fetcher_type == 'forum':
            return fetcher.run_by_url_forum(task.url, task.login_info.__dict__)
        elif task.fetcher_type == 'forum_num':
            platform = self.loader.load_platform(task)
            domain = urllib.parse.urlparse(task.url).netloc
            course_list, login_info = self.mgr_rpc_client.ask_course_list_on(platform, domain)
            return fetcher.run(data=course_list, login_info=login_info)
        else:
            return fetcher.run()

    def run_once(self, task: ScheduleTask) -> dict:
        logging.info("[run_once] start fetch task: %s", task.task_id)
        self.mgr_rpc_client.report_task_start(task.task_id)
        task.login_info = LoginInfo()
        info = self.mgr_rpc_client.ask_login_info(task.task_id, task.login_info.__dict__)
        if info:
            task.login_info.load_info(info)
            logging.info("[crawler.run_once] Get LoginInfo succeeded.")
        else:
            logging.info("[crawler.run_once] There is no LoginInfo.")
        try:
            for i in range(5):
                try:
                    try:
                        # driver = self.create_driver(task)
                        logging.info('[Crawler.run_once] create driver successfully.')
                    except selenium.common.exceptions.WebDriverException as e:
                        logging.error('[Crawler.run_once] cannot create driver. err: [%s]', e)
                        raise e
                    res = {}
                    try:
                        pipeline_cfg = self.loader.get_pipeline_module(task)
                        fetcher = self.loader.load(task)()
                    except Exception as e:
                        logging.error('[Crawler.run_once] cannot load fetcher. err: [%s]', e)
                        logging.error('[Crawler.run_once] %s', traceback.format_exc())
                        raise e
                    res = self.fetch_data(fetcher, task)
                    break
                except OfflineError:  # 这个错误是不是爬虫类里自己抛出？
                    logging.error('Crawler.run_once] Crawler is OFFLINE , ask for login_info..........')
                    info = self.mgr_rpc_client.ask_login_info(task.task_id, task.login_info.__dict__)
                    if info:
                        task.login_info.load_info(info)
                        logging.info("[crawler.run_once] Get ReLoginInfo succeeded.")
                        continue
        # except KeyboardInterrupt as e:
        #     self.mgr_rpc_client.report_task_interrupt(task.task_id)
        #     raise e
        except Exception as e:
            logging.error('[Crawler.run_once] cannot fetch data. err: [%s]', e)
            logging.error('[Crawler.run_once] %s', traceback.format_exc())
            # driver.quit()
            self.mgr_rpc_client.report_task_failed(task.task_id)
            logging.info("[run_once] failed fetch task: %s", task.task_id)
            return res
        for key in res.keys():
            if key == "course_list_info":
                if len(res[key]) < 25:
                    self.mgr_rpc_client.report_task_failed(task.task_id)
                    logging.info("[run_once] failed fetch task: %s,  Get 0 records.", task.task_id)
                    return res
                self.mgr_rpc_client.report_crawl_finish(task.task_id, str(len(res[key])))
            elif key == "forum_post_info":
                if len(res[key]) == 0:
                    self.mgr_rpc_client.report_task_failed(task.task_id)
                    logging.info("[run_once] failed fetch task: %s,  Get 0 records.", task.task_id)
                    return res
                print(key, type(key))
                self.mgr_rpc_client.report_crawl_finish(task.task_id, str(len(res[key])))
            elif key == "forum_num_info":
                if len(res[key]) == 0:
                    self.mgr_rpc_client.report_task_failed(task.task_id)
                    logging.info("[run_once] failed fetch task: %s,  Get 0 records.", task.task_id)
                    return res
                print(key, type(key))
                self.mgr_rpc_client.report_crawl_finish(task.task_id, str(len(res[key])))
            logging.info("[run_once] finish crawl task: %s. Get %r records.", task.task_id, len(res[key]))
        try:
            if res:
                self.pl_rpc_client.put_data(pipeline_cfg['module'], pipeline_cfg['init_args'], task.id, task.task_id,
                                            res)
        except Exception as e:
            logging.error('[Crawler.run_once] cannot put data to pipeline. err: [%s]', e)
            logging.error('[Crawler.run_once] %s', traceback.format_exc())
            self.mgr_rpc_client.report_task_failed(task.task_id)
            return res
        self.mgr_rpc_client.report_task_finish(task.task_id)
        logging.info("[run_once] finish fetch task: %s", task.task_id)

        return res


class CrawlerKeepAliveThread(threading.Thread):
    def __init__(self, crawler: Crawler):
        super().__init__()
        self.crawler = crawler

    def run(self):
        self.setName("CrawlerKeepAliveThread")

        while True:
            time.sleep(30)
            if not self.crawler.is_connected_to_mgr:
                logging.warning('[Heartbeat] connection to Manager broken. trying to connect')
                self.crawler.register()
            else:
                logging.info('[Heartbeat] Heart beats.')
                self.crawler.keep_alive()
