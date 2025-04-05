# ========================= LIBS =========================

import logging

from datetime import datetime
from functools import wraps

from utils.sns_notification_utils import SNSNotification

# ========================= JobLog =========================

class JobLog:
    def __init__(self, args: dict, db_connection: object):
        self.db_connection = db_connection
        self.job_name = args['JOB_NAME']
        self.job_run_id = args['JOB_RUN_ID']
        self.log_schema = args['log_schema']
        self.log_table = args['log_table']

        self.start_time = datetime.now()
        self.batch_id = self.start_time.strftime('%Y%m%d%H%M%S')
        
        # Notification Levels
        #       - None: No notifications
        #       - Info: All notifications
        #       - Warning: Only warning and error notifications
        #       - Error: Only error notifications
        # If NotificationLevel is not provided, then the default value is "None"
        # If SNS Topic ARN is not provided, then NotificationLevel will be set to "None"
        self.notification_level = 'none' if args.get('sns_topic_arn', '') == '' else args.get('notification_level', 'none').lower()
        if self.notification_level not in ['none', 'info', 'warning', 'error']:
            self.notification_level = 'none'
        if self.notification_level != 'none':
            self.sns_client = SNSNotification(args['sns_topic_arn'])

    def __repr__(self):
        return repr({
            'job_name': self.job_name,
            'job_run_id': self.job_run_id,
            'start_time': self.start_time,
            'batch_id': self.batch_id,
            'db_connection': self.db_connection
        })

# ========================= TableLog =========================

class TableLog:
    def __init__(self, job_log: JobLog, database_name: str, table_name: str, config_no: int = 0, subject: str = None, details: str = None):
        self.job_log = job_log

        self.job_name = self.job_log.job_name
        self.job_run_id = self.job_log.job_run_id
        self.batch_id = self.job_log.batch_id
        self.db_connection = self.job_log.db_connection
        self.log_schema = self.job_log.log_schema
        self.log_table = self.job_log.log_table

        self.start_time = datetime.now()

        self.database_name = database_name
        self.table_name = table_name
        self.config_no = config_no

        self.notification_level = self.job_log.notification_level
        if self.notification_level != 'none':
            self.sns_client = self.job_log.sns_client

        self.start(subject, details)

    def start(self, subject: str = None, details: str = None):
        job_status = 'IN-PROGRESS'

        log_details = {
            'start_time': self.start_time,
            'batch_id': self.batch_id,
            'config_no': self.config_no,
            'job_name': self.job_name,
            'job_run_id': self.job_run_id,
            'database_name': self.database_name,
            'table_name': self.table_name,
            'job_status': job_status,
            'last_modified_timestamp': datetime.now()
        }

        result = self.db_connection.insert_sql(
            schema=self.log_schema,
            table=self.log_table,
            insert_details=log_details
        )

        self.notify(job_status, subject, details, self.start_time, '')

        return result

    def warning(self, warning_message: str, subject: str = None, details: str = None):
        job_status = 'WARNED'
        end_time = datetime.now()

        set_values = {
            'job_status': job_status,
            'error_message': warning_message,
            'end_time': end_time,
            'last_modified_timestamp': datetime.now()
        }

        filter_conditions = {
            'batch_id': self.batch_id,
            'job_name': self.job_name,
            'job_run_id': self.job_run_id,
            'database_name': self.database_name,
            'table_name': self.table_name
        }

        result = self.db_connection.update_sql(
            schema=self.log_schema,
            table=self.log_table,
            set_values=set_values,
            filter_conditions=filter_conditions
        )

        self.notify(job_status, subject, details, self.start_time, end_time, warning_message, '')

        return result

    def error(self, error_message: str, subject: str = None, details: str = None):
        job_status = 'FAILED'
        end_time = datetime.now()

        set_values = {
            'job_status': job_status,
            'error_message': error_message,
            'end_time': end_time,
            'last_modified_timestamp': datetime.now()
        }

        filter_conditions = {
            'batch_id': self.batch_id,
            'job_name': self.job_name,
            'job_run_id': self.job_run_id,
            'database_name': self.database_name,
            'table_name': self.table_name
        }

        result = self.db_connection.update_sql(
            schema=self.log_schema,
            table=self.log_table,
            set_values=set_values,
            filter_conditions=filter_conditions
        )

        self.notify(job_status, subject, details, self.start_time, end_time, '', error_message)

        return result

    def success(self, processed_details: dict = {}, subject: str = None, details: str = None):
        job_status = 'SUCCESSFUL'
        end_time = datetime.now()

        set_values = {
            'job_status': job_status,
            'end_time': end_time,
            'last_modified_timestamp': datetime.now()
        }

        set_values.update(processed_details)

        filter_conditions = {
            'batch_id': self.batch_id,
            'job_name': self.job_name,
            'job_run_id': self.job_run_id,
            'database_name': self.database_name,
            'table_name': self.table_name
        }

        result = self.db_connection.update_sql(
            schema=self.log_schema,
            table=self.log_table,
            set_values=set_values,
            filter_conditions=filter_conditions
        )

        self.notify(job_status, subject, details, self.start_time, end_time)

        return result

    def notify(self, job_status: str, subject: str, details: str, start_time, end_time, warning_message=None, error_message=None):
        if not (
            (self.notification_level in ['info'] and job_status.upper() in ['IN-PROGRESS', 'SUCCESSFUL']) or \
            (self.notification_level in ['info', 'warning'] and job_status.upper() in ['WARNED']) or \
            (self.notification_level in ['info', 'warning', 'error'] and job_status.upper() in ['FAILED'])
        ):
            return
        
        subject = f"{job_status}: {self.job_name} for config:{self.config_no}" if not subject else subject

        if not details:
            details_mapping = {
                'IN-PROGRESS': 'The job started execution.',
                'WARNED': f"The job completed/failed with warning: {warning_message}.",
                'FAILED': f"The job failed with error: {error_message}",
                'SUCCESSFUL': 'The job completed execution with no warnings/errors.'
            }
            details = details_mapping.get(job_status.upper(), '')

        s_start_time = str(start_time)
        s_end_time = str(end_time) if end_time and job_status.upper() != 'IN-PROGRESS' else ''

        json_body = {
            'Batch ID': self.batch_id,
            'Config No': self.config_no,
            'Job Name': self.job_name,
            'Job Run ID': self.job_run_id,
            'Database': self.database_name,
            'Table': self.table_name,
            'Job Start Time': s_start_time,
            'Job End Time': s_end_time,
            'Details': details
        }

        self.sns_client.notification_email(subject, json_body)

# ========================= script_logger =========================

def script_logger(job_args: dict):

    def decorator(ETLObject):

        @wraps(ETLObject)
        def wrapper(*args, **kwargs):
            formatter = logging.Formatter('%(levelname)s - %(asctime)s - %(filename)s:%(lineno)s - %(funcName)s(): %(message)s')
            logger = logging.getLogger(f"{job_args['JOB_NAME']}")
            logger.setLevel(logging.INFO)
            stream_handler = logging.StreamHandler()
            stream_handler.setFormatter(formatter)
            logger.addHandler(stream_handler)

            return ETLObject(*args, **kwargs, logger=logger)

        return wrapper

    return decorator
