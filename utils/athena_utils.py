import boto3
import logging


formatter = logging.Formatter('%(levelname)s - %(asctime)s - %(filename)s:%(lineno)s: %(message)s')
logger = logging.getLogger('athena_utils')
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger.addHandler(handler)


class AthenaAccess:
    def __init__(self, catalog_name: str = 'AwsDataCatalog'):
        self.athena_client = boto3.client('athena')
        self.catalog_name = catalog_name

    def get_table_metadata(self, database: str, table: str) -> dict:
        """
        Get table metadata from AWS Athena
        """
        try:
            response = self.athena_client.get_table_metadata(
                CatalogName=self.catalog_name,
                DatabaseName=database,
                TableName=table
            )
            table_metadata = response['TableMetadata']
            return table_metadata
        except Exception as e:
            logger.exception(f"Error getting metadata for table \"{database}.{table}\": {e}")
            return None

    def get_table_columns(self, database: str, table: str) -> list:
        """
        Get column names and column data types of a table from AWS Athena
        """
        try:
            table_metadata = self.get_table_metadata(database, table)
            columns = table_metadata.get('Columns', [])
            return columns
        except Exception as e:
            logger.exception(f"Error getting columns for table \"{database}.{table}\": {e}")
            return None

    def get_table_column_names(self, database: str, table: str) -> list:
        """
        Get column names of a table from AWS Athena
        """
        try:
            columns = self.get_table_columns(database, table)
            column_names = [column['Name'] for column in columns]
            return column_names
        except Exception as e:
            logger.exception(f"Error getting column names for table \"{database}.{table}\": {e}")
            return None

    def table_exists(self, database: str, table: str) -> bool:
        """
        Check if a table exists in AWS Athena
        """
        try:
            table_metadata = self.get_table_metadata(database, table)
            table_exists = table_metadata is not None
            return table_exists
        except Exception as e:
            logger.exception(f"Error checking if table \"{database}.{table}\" exists: {e}")
            return False
