import unittest
import selenium.webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

from crawler.fetcher.icourse163 import VideoFetcher


class MyTestCase(unittest.TestCase):
    def test_fetch_forum_info(self):
        url = 'https://www.icourse163.org/learn/HIT-1001515007#/learn/content'
        url = 'https://www.icourse163.org/learn/BUPT-1003557006#/learn/content'
        caps = DesiredCapabilities.CHROME
        caps['loggingPrefs'] = {'performance': 'ALL'}
        options = selenium.webdriver.ChromeOptions()
        # TODO 貌似Headless模式被icourse163反爬了，headless加载不上JS渲染的页面
        prefs = {"profile.managed_default_content_settings.images": 2}
        options.add_experimental_option("prefs", prefs)
        # options.add_argument('headless')
        options.add_argument('disable-gpu')
        options.add_experimental_option('w3c', False)
        browser = selenium.webdriver.Chrome(desired_capabilities=caps, options=options)
        browser.get(url)
        fetcher = VideoFetcher(browser)
        res = fetcher.run()
        browser.quit()

        self.assertEqual(True, len(res['forum_post_info']) > 100)


if __name__ == '__main__':
    unittest.main()
