import os
import smtplib
from datetime import date
from email import encoders
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


def read_file_contents(file_path) -> str:
    with open(file_path, encoding="utf-8") as f:
        read_data = f.read()
    return read_data

    
class ResourceHelper:

    def get_data_json_path(self):
        return os.path.abspath(os.path.join(os.path.dirname( __file__ ), 'image-processing-data.json'))

class EmailHelper:

    def send_crawler_error_email(self):

        recipients = ["babatunde.adebola.adewuyi@census.gov", "richard.lee.flagg@census.gov"]
        subject = "DCDL Image Processing Crawler Error"
        body = """
            There has been an error in the DCDL Image Processing application. 
            Please review the attached log file and take appropriate action
        """
        attachments = ['crawler.log']

        self.send_email(recipients, subject, body, attachments)

    def send_crawl_completed_email(self, message, census_year):

        recipients = ["babatunde.adebola.adewuyi@census.gov", "richard.lee.flagg@census.gov"]
        subject = f"DCDL Image Processing {census_year} Crawler completed"
        body = message
        attachments = ['crawler.log']

        self.send_email(recipients, subject, body, attachments)


    def send_email(self, recipients, subject: str, body:str, attachments = []):

        smtp_server = "mailout.census.gov"
        sender = "dcdl-imageprocessing-crawler@census.gov"

        message = MIMEMultipart()
        message["From"] = sender
        message['To'] = ", ".join(recipients)
        message['Subject'] = subject

        body = MIMEText(body) # convert the body to a MIME compatible string
        message.attach(body) # attach it to your main message

        if len(attachments) > 0:
            for file in attachments:
                attachment = open(file,'rb')
                obj = MIMEBase('application','octet-stream')
                obj.set_payload((attachment).read())
                encoders.encode_base64(obj)
                obj.add_header('Content-Disposition',"attachment; filename= "+file)
                message.attach(obj)                

        mail_message = message.as_string()

        email_session = smtplib.SMTP(smtp_server)
        email_session.sendmail(sender, recipients ,mail_message)
        email_session.quit()

def sizeof_fmt(num, suffix="B"):
    for unit in ("", "K", "M", "G", "T", "P", "E", "Z"):
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"

