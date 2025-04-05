# ========================= LIBS =========================

import json
import logging
import sys
import pg8000.dbapi
import pg8000.native

from datetime import datetime
from pandas import DataFrame

# ========================= LOGGER =========================

formatter = logging.Formatter('%(levelname)s - %(asctime)s - %(filename)s:%(lineno)s - %(funcName)s(): %(message)s')
logger = logging.getLogger('postgresql_utils')
logger.setLevel(logging.INFO)
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

# ========================= RDSAccess =========================

class RDSAccess:
    def __init__(self, secret_name: str, region_name: str, boto3: object):
        self.secret_name = secret_name
        self.region_name = region_name
        self.boto3 = boto3
        self.secret, self.url = self.get_rds_secret()

    def get_rds_secret(self):
        """
        To get secret values for RDS connection
        """
        secret_name = self.secret_name
        region_name = self.region_name
        
        secrets_manager_client = self.boto3.client('secretsmanager', region_name=region_name)

        try:
            response = secrets_manager_client.get_secret_value(SecretId=secret_name)
            logger.info('Get RDS secret successfully')
        except Exception as e:
            logger.exception(f"An Exception occurred when getting secret values: {str(e)}")

        secret = response['SecretString']
        secret = json.loads(secret)

        url = (f"{secret['host']}:{secret['port']}/{secret['dbname']}")

        return secret, url

    def establish_postgresql_connection(self):
        host = self.secret['host']
        port = self.secret['port']
        database = self.secret['dbname']
        user = self.secret['username']
        password = self.secret['password']

        try:
            postgresql_connection = pg8000.dbapi.Connection(
                user,
                password=password,
                host=host,
                database=database,
                port=port
            )
            logger.info('PosgreSQL Connection Established')
            return postgresql_connection
        except ConnectionError as ce:
            logger.exception(f"A ConnectionError occurred when establishing postgresql connection: {str(ce)}")
            raise ConnectionError('A ConnectionError occurred when establishing postgresql connection')
        except Exception as e:
            logger.exception(f"An Exception occurred when establishing postgresql connection: {str(e)}")
            raise Exception('An Exception occurred when establishing postgresql connection')

    def read_from_postgresql(self, query: str):
        logger.info(f"Start executing read query: {query}")

        try:
            postgresql_connection = self.establish_postgresql_connection()
            postgresql_cursor = postgresql_connection.cursor()

            postgresql_cursor.execute(query)
            columns = [col[0] for col in postgresql_cursor.description]
            result_df = DataFrame(data=postgresql_cursor.fetchall(), columns=columns)

            logger.info(f"Read query executed successfully")

            return result_df
        except Exception as e:
            logger.exception(f"An Exception occurred when reading from postgresql: {str(e)}")
            raise Exception('An Exception occurred when reading from postgresql')

    def write_to_postgresql(self, dataframe: DataFrame, table_name: str):
        _, url = self.get_rds_secret()
        connection_url = f"jdbc:postgresql://{url}"
        connection_details = {
            'user': self.secret['username'],
            'password': self.secret['password']
        }

        logger.info(f"Start writing to postgresql table: {table_name}")

        try:
            dataframe.write.jdbc(
                url=connection_url,
                table=table_name,
                mode='append',
                properties=connection_details
            )

            logger.info(f"DataFrame written to {table_name} successfully")
        except Exception as e:
            logger.exception(f"An Exception occurred when writing to postgresql table {table_name}: {str(e)}")
            raise Exception('An Exception occurred when writing to postgresql table {table_name}')

    def execute_postgresql_dml(self, query: str):
        try:
            postgresql_connection = self.establish_postgresql_connection()
            postgresql_cursor = postgresql_connection.cursor()

            logger.info(f"Start executing DML query: {query}")
            postgresql_cursor.execute(query)
            logger.info(f"DML query executed successfully. Number of rows affected: {postgresql_cursor.rowcount}")

            postgresql_connection.commit()
        except Exception as e:
            logger.exception(f"An Exception occurred when executing DML query: {str(e)}")
            raise Exception('An Exception occurred when executing DML query')

    def execute_multiple_postgresql_dml(self, query_list: list):
        try:
            postgresql_connection = self.establish_postgresql_connection()
            postgresql_cursor = postgresql_connection.cursor()

            for index, query in enumerate(query_list):
                logger.info(f"Start executing DML queries {index}: {query}")
                postgresql_cursor.execute(query)
                logger.info(f"Multiple DML queries executed successfully. Number of rows affected: {postgresql_cursor.rowcount}")

            postgresql_connection.commit()
        except Exception as e:
            logger.exception(f"An Exception occurred when executing multiple DML queries: {str(e)}")
            raise Exception('An Exception occurred when executing multiple DML queries')

    def map_datatype(self, value: str):
        none_type = type(None)
        data_type = {
            str: f"'{value}'",
            datetime: f"TIMESTAMP '{value}'",
            none_type: 'null'
        }
        result = data_type.get(type(value), value)
        return result

    def build_set_clause(self, set_values: dict):
        set_list = []

        for column, value in set_values.items():
            set_list.append(f"{column} = {self.map_datatype(value)}")
        
        set_clause = ',\n\t'.join(set_list)

        return (set_clause)

    def build_filter_clause(self, filter_conditions: dict):
        filter_list = []

        for column, value in filter_conditions.items():
            if self.map_datatype(value) == 'null':
                filter_list.append(f"AND {column} IS NULL")
            else:
                filter_list.append(f"AND {column} = {self.map_datatype(value)}")
        
        filter_clause = '\n'.join(filter_list)

        return (filter_clause)

    def insert_query(self, schema: str, table: str, insert_details: dict):
        column_list = []
        value_list = []

        for column, value in insert_details.items():
            column_list.append(f"{column}")
            value_list.append(f"{self.map_datatype(value)}")

        column_clause = ', '.join(column_list)
        value_clause = ', '.join(value_list)

        insert_query = f"""INSERT INTO {schema}.{table} ({column_clause}) VALUES ({value_clause})"""

        self.execute_postgresql_dml(insert_query)

        return insert_query

    def update_query(self, schema: str, table: str, set_values: dict, filter_conditions: dict):
        set_clause = self.build_set_clause(set_values)
        filter_clause = self.build_filter_clause(filter_conditions)

        update_query = f"""UPDATE {schema}.{table} SET {set_clause} WHERE 1=1 {filter_clause}"""

        self.execute_postgresql_dml(update_query)

        return update_query

    def select_sql(self, query: str):
        try:
            postgresql_connection = self.establish_postgresql_connection()
            postgresql_cursor = postgresql_connection.cursor()

            logger.info(f"Start executing select query: {query}")

            postgresql_cursor.execute(query)
            columns = [col[0] for col in postgresql_cursor.description]
            result_df = DataFrame(data=postgresql_cursor.fetchall(), columns=columns)

            logger.info(f"Select query executed successfully")

            return result_df
        except Exception as e:
            logger.exception(f"An Exception occurred when executing select query: {str(e)}")
            raise Exception('An Exception occurred when executing select query')

    def bulk_insert_sql(self, query: str, insert_list: list):
        try:
            postgresql_connection = self.establish_postgresql_connection()
            postgresql_cursor = postgresql_connection.cursor()

            logger.info(f"Start executing bulk insert query: {query}")
            postgresql_cursor.executemany(query, insert_list)
            logger.info(f"Bulk insert query executed successfully. Number of rows affected: {postgresql_cursor.rowcount}")

            postgresql_connection.commit()
        except Exception as e:
            logger.exception(f"An Exception occurred when executing bulk insert query: {str(e)}")
            raise Exception('An Exception occurred when executing bulk insert query')

    def bulk_update_sql(self, query: str, update_list: list):
        try:
            postgresql_connection = self.establish_postgresql_connection()
            postgresql_cursor = postgresql_connection.cursor()

            logger.info(f"Start executing bulk update query: {query}")
            postgresql_cursor.executemany(query, update_list)
            logger.info(f"Bulk update query executed successfully. Number of rows affected: {postgresql_cursor.rowcount}")

            postgresql_connection.commit()
        except Exception as e:
            logger.exception(f"An Exception occurred when executing bulk update query: {str(e)}")
            raise Exception('An Exception occurred when executing bulk update query')
