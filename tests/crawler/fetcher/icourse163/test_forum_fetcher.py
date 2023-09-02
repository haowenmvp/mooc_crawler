import unittest

import selenium.webdriver

from crawler.fetcher.icourse163 import ForumFetcher


class MyTestCase(unittest.TestCase):
    def test_fetch_forum_info(self):
        url = 'https://www.icourse163.org/learn/HIT-1001515007#/learn/forumindex'
        options = selenium.webdriver.ChromeOptions()
        # TODO 貌似Headless模式被反爬了，headless加载不上JS渲染的页面
        # options.add_argument('headless')
        browser = selenium.webdriver.Chrome(options=options)
        browser.get(url)
        fetcher = ForumFetcher(browser)
        res = fetcher.run()
        browser.quit()

        self.assertEqual(True, len(res['forum_post_info']) > 100)

    def test_fetch_forum_url_list(self):
        url = 'https://www.icourse163.org/learn/HIT-1001515007#/learn/forumindex'
        options = selenium.webdriver.ChromeOptions()
        # TODO 貌似Headless模式被反爬了，headless加载不上JS渲染的页面
        # options.add_argument('headless')
        browser = selenium.webdriver.Chrome(options=options)

        browser.get(url)
        fetcher = ForumFetcher(browser)
        res = fetcher.get_forum_url_list()
        browser.quit()

        self.assertEqual(True, len(res) > 100)

    def test_fetch_forum_detail(self):
        url = 'https://www.icourse163.org/learn/HIT-1001515007#/learn/forumdetail?pid=1214121615'
        options = selenium.webdriver.ChromeOptions()
        # TODO 貌似Headless模式被反爬了，headless加载不上JS渲染的页面
        # options.add_argument('headless')
        browser = selenium.webdriver.Chrome(options=options)

        browser.get(url)
        fetcher = ForumFetcher(browser)
        res = fetcher.get_forum_detail(url)
        browser.quit()

        self.assertEqual(True, len(res) > 0)


if __name__ == '__main__':
    unittest.main()
