import unittest

from manager.login_info_mgr import LoginInfoManager


class LoginInfoMgrTestCase(unittest.TestCase):
    def test_do_login(self):
        login_data = [
            {
                "domain": "www.icourse163.org",
                "launcher": "manager.launchers.ICourse163Launcher",
                "login_url": "https://www.icourse163.org/member/login.htm",
                "login_infos": [
                    {"username": "", "password": ""},
                ]
            }
        ]
        mgr = LoginInfoManager(login_data)
        res = mgr.do_login()

        assert res

    def test_haodaxue_do_login(self):
        login_data = [
            {
                "domain": "www.cnmooc.org",
                "launcher": "manager.launchers.HaodaxueLauncher",
                "login_url": "http://www.cnmooc.org/home/login.mooc",
                "login_infos": [
                    {"username": "443786959@qq.com", "password": "likaijie145banD4"}
                ]
            }
        ]
        mgr = LoginInfoManager(login_data)
        res = mgr.do_login()
        cookies = res['www.cnmooc.org'][0].cookies

        # cookies = [{'name': 'BEC', 'value': 'b5ebcb9150bc89c2a3904dcff7d31333', 'path': '/', 'domain': 'www.cnmooc.org', 'secure': False, 'httpOnly': False, 'expiry': 1574848131},
        #            {'name': 'moocvk', 'value': 'df6c4255c97540a19a0f810152ba694a', 'path': '/', 'domain': 'www.cnmooc.org', 'secure': False, 'httpOnly': True, 'expiry': 3151646225},
        #            {'name': 'JSESSIONID', 'value': '864ACD6B4745D483A0F78CB45D32DDA4.tomcat-host1-4', 'path': '/', 'domain': 'www.cnmooc.org', 'secure': True, 'httpOnly': True},
        #            {'name': 'Hm_lvt_ed399044071fc36c6b711fa9d81c2d1c', 'value': '1574846334', 'path': '/', 'domain': '.cnmooc.org', 'secure': False, 'httpOnly': False, 'expiry': 1606382333},
        #            {'name': 'Hm_lpvt_ed399044071fc36c6b711fa9d81c2d1c', 'value': '1574846334', 'path': '/', 'domain': '.cnmooc.org', 'secure': False, 'httpOnly': False}]
        from selenium.webdriver import Chrome, ChromeOptions
        options = ChromeOptions()
        # user_agent = """Mozilla/5.0 (X11; Linux x86_64; rv:60.0) Gecko/20100101 Firefox/60.0"""
        # options.add_argument('--user-agent="%s"' % user_agent)
        driver = Chrome(options=options)
        driver.get("https://www.cnmooc.org/portal/course/4920/13261.mooc")
        for cookie in cookies:
            if 'expiry' in cookie:
                del cookie['expiry']
            driver.add_cookie(cookie)
        driver.refresh()
        driver.refresh()

        assert 1


if __name__ == '__main__':
    unittest.main()
