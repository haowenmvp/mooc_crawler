import logging
import selenium.common.exceptions

from selenium.webdriver.support.ui import WebDriverWait

from persistence.model.task import LoginInfo
from manager.launchers.base_launcher import BaseLauncher


class XueyinLauncher(BaseLauncher):
    def login(self, username='', password='', qr_path='') -> LoginInfo:
        login_info = LoginInfo()
        if username and password:
            self.login_process_using_password(username, password)
            login_info.login_data['username'] = username
            login_info.login_data['password'] = password
        elif qr_path:
            pass
        else:
            raise ValueError("Must provide username/password or login QR image path.")

        login_info.cookies = self.driver.get_cookies()
        return login_info

    def login_process_using_password(self, username: str, password: str):
        try:
            div_login = WebDriverWait(self.driver, self.kWaitSecond).until(
                lambda x: x.find_element_by_class_name('loginBox'))
        except selenium.common.exceptions.TimeoutException:
            logging.warning("[XueyinLauncher.login_process_using_password] Cannot find login div")
            return

        input_username = div_login.find_element_by_id('phone')
        input_password = div_login.find_element_by_id('password')

        input_username.send_keys(username)
        input_password.send_keys(password)

        self.driver.execute_script("document.getElementsByName('button')[0].click()")
