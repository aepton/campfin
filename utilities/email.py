import boto3
import logging

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from settings import settings

logger = logging.getLogger(__name__)

def send_email(
        from_address=settings.EMAIL_FROM_ADDRESS,
        to_addresses=[],
        subject='',
        body=''):

    session = boto3.Session(profile_name=settings.AWS_PROFILE_NAME)
    connection = session.client('ses', settings.AWS_REGION)

    for address in to_addresses:
        message = MIMEMultipart('alternative')
        message['Subject'] = subject
        message['From'] = from_address
        message['To'] = address

        part_text = MIMEText(body, 'plain')
        message.attach(part_text)

        part_html = MIMEText(body, 'html')
        message.attach(part_html)

        logger.info('Emailing %s' % address)
        connection.send_raw_email(
            RawMessage={
                'Data': message.as_string()
            },
            Source=message['From'],
            Destinations=[message['To']])
