import boto3
import json
import logging
import smtplib

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


formatter = logging.Formatter('%(levelname)s - %(asctime)s - %(filename)s:%(lineno)s: %(message)s')
logger = logging.getLogger('email_notification_utils')
logger.setLevel(logging.INFO)
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)


class SNSNotification:
    def __init__(self, sns_topic_arn: str):
        self.sns_client = boto3.client('sns')
        self.sns_topic_arn = sns_topic_arn

    def send_email(self, subject: str, json_body: json):
        """
        Parameters:
            subject (str): Subject of the email
            json_body (json): Body of the email in JSON format
        """
        try:
            json_data = json.dumps(json_body, indent=4, separators=(',', ':'))

            response = self.sns_client.publish(
                TopicArn=self.sns_topic_arn,
                Subject=subject,
                Message=json_data
            )

            logger.info('Email sent successfully')
        except Exception as e:
            logger.exception(f"Error sending email by SNS: {str(e)}")


class SMTPNotification:
    def __init__(self, smtp_server: str, smtp_port: int):
        self.smtp_server: str = smtp_server
        self.smtp_port: int = smtp_port

    def send_email(self, subject: str, body: str, sender: str, recipients: list = [], cc_recipients: list = []):
        """
        Parameters:
            subject (str): Subject of the email
            body (str): Body of the email
            sender (str): Sender email address
            recipients (list): List of recipient email addresses
            cc_recipients (list): List of cc recipient email addresses
        """
        try:
            msg = MIMEMultipart()
            msg['From'] = sender
            msg['To'] = ', '.join(recipients)
            msg['Cc'] = ', '.join(cc_recipients)
            msg['Subject'] = subject

            msg.attach(MIMEText(body, 'plain'))

            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.sendmail(sender, recipients + cc_recipients, msg.as_string())

            logger.info('Email sent successfully')
        except Exception as e:
            logger.exception(f"Error sending email by SMTP: {str(e)}")
