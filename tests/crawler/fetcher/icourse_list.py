from crawler.fetcher.icourse163.list_fetcher import ListFetcher
from utils.utils import process_course_list_info
import selenium.webdriver
import selenium.common.exceptions
import pickle
import sys
import os


sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# from selenium.webdriver.remote.webdriver import WebDriver
# from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
#
# caps = DesiredCapabilities.CHROME
# caps['loggingPrefs'] = {'performance': 'ALL'}
# options = selenium.webdriver.ChromeOptions()
# # TODO 貌似Headless模式被icourse163反爬了，headless加载不上JS渲染的页面
# prefs = {"profile.managed_default_content_settings.images": 2}
# options.add_experimental_option("prefs", prefs)
# # options.add_argument('headless')
# options.add_argument('disable-gpu')
# options.add_experimental_option('w3c', False)
# driver = selenium.webdriver.Chrome(options=options, desired_capabilities=caps)
# driver.get('https://www.icourses.cn')
# fetcher = ListFetcher(driver)
# data = fetcher.run()
# with open('./icourse_later.pkl','wb') as f:
#     pickle.dump(data,f)
# process_course_list_info(data)

with open('../../../22.pkl','rb') as f:
    data = pickle.load(f)
process_course_list_info(data)

# for course in data:
#     print(course)
