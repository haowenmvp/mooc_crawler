import unittest

import selenium.webdriver

from crawler.fetcher.xueyinonline import ForumFetcher


class MyTestCase(unittest.TestCase):
    def test_fetch_forum_info(self):
        url = 'http://www.xueyinonline.com/detail/204399950'
        options = selenium.webdriver.ChromeOptions()
        options.add_argument('headless')
        browser = selenium.webdriver.Chrome(options=options)
        browser.get(url)
        fetcher = ForumFetcher(browser)
        res = fetcher.run()
        browser.quit()

        self.assertEqual(True, len(res['forum_post_info']) > 100)


if __name__ == '__main__':
    unittest.main()
