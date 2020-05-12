from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from news_parser.settings import EMAIL_MANAGER
import smtplib
import mimetypes


class EmailManager:
    @classmethod
    def _make_email_body(cls, sender: str, recipients: str, title: str, fname: str):
        msg = MIMEMultipart()
        msg["From"] = sender
        msg["To"] = recipients
        msg["Subject"] = title

        body = "Отчет"
        msg.attach(MIMEText(body, "plain", "utf-8"))

        with open(fname, "rb") as f:
            ctype, _ = mimetypes.guess_type(fname)
            maintype, subtype = ctype.split("/", 1)
            file = MIMEBase(maintype, subtype)
            file.set_payload(f.read())

        encoders.encode_base64(file)
        file.add_header('Content-Disposition', 'attachment', filename=fname)
        msg.attach(file)

        return msg

    @classmethod
    def send_report(cls, fname: str, recipients: str):
        addr_to = recipients
        addr_from = EMAIL_MANAGER["login"]

        email_body = cls._make_email_body(
            sender=addr_from,
            recipients=addr_to,
            title="Отчет news_parser",
            fname=fname
        )

        server = smtplib.SMTP('smtp.gmail.com', 587)
        # server.set_debuglevel(True)
        server.starttls()
        server.login(addr_from, EMAIL_MANAGER["password"])
        server.send_message(email_body)
        server.quit()
