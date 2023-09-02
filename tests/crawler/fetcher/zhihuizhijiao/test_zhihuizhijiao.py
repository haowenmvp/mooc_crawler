from crawler.fetcher.zhihuizhijiao.list_fetcher import ListFetcher
import selenium.webdriver
import selenium.common.exceptions
import pickle
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities


caps = DesiredCapabilities.CHROME
caps['loggingPrefs'] = {'performance': 'ALL'}
options = selenium.webdriver.ChromeOptions()
prefs = {"profile.managed_default_content_settings.images": 2}
options.add_experimental_option("prefs", prefs)
options.add_argument('headless')
options.add_argument('disable-gpu')
options.add_experimental_option('w3c', False)
driver = selenium.webdriver.Chrome(options=options, desired_capabilities=caps)
fetcher = ListFetcher(driver)
