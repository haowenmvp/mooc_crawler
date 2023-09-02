import selenium.webdriver
import selenium.common.exceptions

from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

def create_firefox_driver() -> WebDriver:
    options = selenium.webdriver.FirefoxOptions()
    options.add_argument("--headless")
    options.set_preference('permissions.default.image', 2)
    options.add_argument("--disable-gpu")
    return selenium.webdriver.Firefox(options=options)

def create_chrome_driver() -> WebDriver:
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
url = 'https://www.baidu.com'
#firefox = create_firefox_driver()
chrome = create_chrome_driver()
#firefox.get(url)
chrome.get(url)
#print(firefox.title)
print(chrome.title)
