import logging
import selenium.common.exceptions

from selenium.webdriver.support.ui import WebDriverWait

from persistence.model.task import LoginInfo
from manager.launchers.base_launcher import BaseLauncher


class ICourse163Launcher(BaseLauncher):
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
                lambda x: x.find_element_by_class_name('ux-urs-login-urs-tab-login'))
        except selenium.common.exceptions.TimeoutException:
            logging.warning("[ICourse163Launcher.login_process_using_password] Cannot find login div")
            return

        div_login.find_element_by_class_name('ux-tabs-underline').find_elements_by_tag_name('li')[-1].click()

        div_login_form = div_login.find_element_by_class_name('icourse-login-form')
        input_username = div_login_form.find_element_by_class_name('account-field').find_element_by_tag_name('input')
        input_password = div_login_form.find_element_by_class_name('password-field').find_element_by_tag_name('input')
        btn_login = div_login_form.find_element_by_class_name('button-field').find_element_by_tag_name('span')

        input_username.send_keys(username)
        input_password.send_keys(password)
        btn_login.click()
        try:
            bind_dialog = WebDriverWait(self.driver, self.kWaitSecond).until(
                lambda x: x.find_element_by_class_name('bind-phone-modal'))


        # except selenium.common.exceptions.TimeoutException:
        except Exception as e:
            logging.info("[ICourse163Launcher.login_process_using_password] No bind dialog.[%s]",e)
            return
        WebDriverWait(self.driver, self.kWaitSecond).until(
            lambda x: x.find_element_by_css_selector("[class='ignore-bind f-pa']")).click()
        logging.info("[ICourse163Launcher.login_process_using_password] Bind dialog,do not bind.")
