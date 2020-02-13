"""Utilities for sending email."""
import smtplib
import subprocess
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from os.path import basename
from typing import List


def send_from_gmail(send_to: List[str], subject: str, text: str, html='',
                    files=[], *, from_address='esa.rss.team@gmail.com',
                    from_username='esa.rss.team',
                    from_password='ioehroher34yc844nywrckd',
                    from_smtp='smtp.gmail.com') -> None:
    """Send email from RSS gmail account

    :param send_to: list of strings containing email addresses
    :param subject: subject of the email
    :param text: body contents of email
    :param html: contents of the body in HTML format
    :param files: list of files to attach
    :param from_address: email address to identify the sender
    :param from_username: username of the sender SMTP
    :param from_password: password of the sender SMTP
    :param from_smtp: SMTP server for the sender
    :return:
    """
    assert isinstance(send_to, list)

    if html:
        msg = MIMEMultipart('alternative')
        msg.attach(MIMEText(text, 'plain'))
        msg.attach(MIMEText(html, 'html'))
    else:
        msg = MIMEMultipart()
        msg.attach(MIMEText(text))
    msg['From'] = from_address
    msg['Subject'] = subject

    for f in files or []:
        with open(f, "rb") as fil:
            msg.attach(MIMEApplication(
                fil.read(),
                Content_Disposition='attachment; filename="%s"' % basename(f),
                Name=basename(f)
            ))

    smtp = smtplib.SMTP_SSL(from_smtp)
    smtp.ehlo()
    smtp.login(from_username, from_password)
    smtp.sendmail(from_address, send_to, msg.as_string())
    smtp.close()


def send_from_mutt(send_to, subject, body_html) -> None:
    assert isinstance(send_to, list)

    cmd = f'cat {body_html} | mutt -e "set content_type=text/html" -s "{subject}" {", ".join(send_to)}'
    subprocess.run(cmd, stdout=subprocess.PIPE, shell=True)
