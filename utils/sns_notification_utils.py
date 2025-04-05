# ========================= LIBS =========================

import boto3
import json
import logging
import sys

# ========================= LOGGER =========================

formatter = logging.Formatter('%(levelname)s - %(asctime)s - %(filename)s:%(lineno)s - %(funcName)s(): %(message)s')
logger = logging.getLogger('sns_notification_utils')
logger.setLevel(logging.INFO)
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

# ========================= SNSNotification =========================

class SNSNotification:
    def __init__(self, sns_topic_arn: str):
        self.sns_client = boto3.client('sns')
        self.sns_topic_arn = sns_topic_arn

    def notification_email(self, subject: str, json_body: json):
        try:
            if len(subject) > 99:
                logger.info(f"Current subject is too long -> Trimming email subject to 99 characters")
                subject = subject[:99]
            custom_subject = str(subject)
            json_data = json.dumps(json_body, indent=4, separators=(",", " : "))

            self.sns_client.publish(
                TopicArn=self.sns_topic_arn,
                Subject=custom_subject,
                Message=json_data
            )

            logger.info('Notification email sent')
        except ConnectionError as ce:
            logger.exception(f"A ConnectionError occurred when sending email notification: {str(ce)}")
            raise ConnectionError('A ConnectionError occurred when sending email notification')
        except Exception as e:
            logger.info(f"An Exception occurred when sending email notification: {str(e)}")
            raise Exception('An Exception occurred when sending email notification')
