import boto3
import json
import logging


formatter = logging.Formatter('%(levelname)s - %(asctime)s - %(filename)s:%(lineno)s: %(message)s')
logger = logging.getLogger('athena_utils')
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger.addHandler(handler)


def get_secret(region_name: str, secret_name: str) -> dict:
    secrets_manager_client = boto3.client('secretsmanager', region_name=region_name)

    try:
        response = secrets_manager_client.get_secret_value(SecretId=secret_name)
        secret = json.loads(response['SecretString'])

        logger.info(f"Secret retrieved successfully for \"{secret_name}\"")

        return secret
    except Exception as e:
        logger.exception(f"Error getting secret for \"{secret_name}\": {e}")
        return {}
