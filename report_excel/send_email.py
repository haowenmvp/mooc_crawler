import logging
import smtplib
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header

from persistence.model.report_config import ReportConfig


class Email(object):
    def __init__(self, config: dict):
        self.sender = config["sender_email"]
        self.password = config["password"]
        self.receiver = list()
        for key in config["receiver_emails"]:
            self.receiver.append(config["receiver_emails"][key])
        self.From = config["From"]
        pass

    def send_with_excel(self, subject: str, main_text: str, excel_path: str, excel_name: str):
        try:
            message = MIMEMultipart()
            message['From'] = Header(self.From, 'utf-8')
            message['To'] = Header(','.join(self.receiver), 'utf-8')
            message['Subject'] = Header(subject, 'utf-8')
            message.attach(MIMEText(main_text, 'plain', 'utf-8'))
            att1 = MIMEApplication(open(excel_path, 'rb').read())
            att1.add_header('Content-Disposition', 'attachment', filename=excel_name)
            message.attach(att1)
            server = smtplib.SMTP_SSL("smtp.qq.com", 465)
            server.login(self.sender, self.password)
            server.sendmail(self.sender, self.receiver, message.as_string())
            server.quit()
            logging.info("[send email]:" + excel_name + "：send successfully")
        except Exception:
            logging.info("[send email]:" + excel_name + "：send failed")
        pass