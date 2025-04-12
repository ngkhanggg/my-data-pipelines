import logging
import pg8000

from datetime import datetime
from pandas import DataFrame
from typing import Any


formatter = logging.Formatter('%(levelname)s - %(asctime)s - %(filename)s:%(lineno)s: %(message)s')
logger = logging.getLogger('database_utils')
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger.addHandler(handler)


class PostgreSQLUtils:
    def __init__(self, host: str, port: int | str, database: str, user: str, password: str):
        self.host: str = host
        self.port: int | str = port
        self.database: str = database
        self.user: str = user
        self.password: str = password

        self.postgresql_connection: pg8000.dbapi.Connection = self.establish_postgresql_connection()
        self.postgresql_jdbc_url: str = self.get_postgresql_jdbc_url()

    def __repr__(self):
        return {
            'host': self.host,
            'port': self.port,
            'database': self.database,
            'jdbc_url': self.postgresql_jdbc_url
        }

    def establish_postgresql_connection(self) -> pg8000.dbapi.Connection:
        """
        Establish a connection to PostgreSQL using pg8000
        """
        try:
            postgresql_connection = pg8000.dbapi.Connection(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )

            logger.info('PostgreSQL connection established successfully')

            return postgresql_connection
        except Exception as e:
            logger.exception(f"Error establish connection to PostgreSQL: {e}")
            raise e

    def get_postgresql_jdbc_url(self) -> str:
        """
        Format given PostgreSQL connection details (host, port, database name) into a JDBC URL
        """
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"

    def execute_postgresql_dml_query(self, query: str):
        try:
            postgresql_cursor = self.postgresql_connection.cursor()

            logger.info(f"Executing DML query: {query}")

            postgresql_cursor.execute(query)  # Execute the DML query
            self.postgresql_connection.commit()  # Commit the changes

            rows_affected = postgresql_cursor.rowcount  # Get the number of rows affected

            logger.info(f"DML query executed successfully, number of rows affected: {rows_affected}")
        except Exception as e:
            logger.exception(f"Error executing DML query: {e}")

    def map_data_type(self, value: Any) -> str:
        none_type = type(None)

        mapping = {
            str: f"'{value}'",
            datetime: f"TIMESTAMP '{value}'",
            none_type: 'NULL'
        }

        return mapping.get(type(value), value)

    def build_set_clause(self, set_values: dict) -> str:
        set_list = []

        for column, value in set_values.items():
            set_list.append(f"{column} = {self.map_data_type(value)}")

        str_set_clause = ',\n\t'.join(set_list)

        return str_set_clause

    def build_where_clause(self, filter_conditions: dict) -> str:
        filter_list = []

        for column, value in filter_conditions.items():
            if self.map_data_type(value) == 'NULL':
                filter_list.append(f"AND {column} IS NULL")
            else:
                filter_list.append(f"AND {column} = {self.map_data_type(value)}")

        where_clause = '\n'.join(filter_list)

        return where_clause

    def select_sql(self, query: str) -> DataFrame:
        try:
            postgresql_cursor = self.postgresql_connection.cursor()

            logger.info(f"Executing SELECT query: {query}")

            postgresql_cursor.execute(query)

            columns = [column[0] for column in postgresql_cursor.description]
            result_df = DataFrame(data=postgresql_cursor.fetchall(), columns=columns)

            logger.info(f"SELECT query executed successfully")

            return result_df
        except Exception as e:
            logger.exception(f"Error executing SELECT query: {e}")
            return None

    def insert_sql(self, table: str, insert_details: dict) -> str:
        list_columns = []
        list_values = []

        for column, value in insert_details.items():
            list_columns.append(column)
            list_values.append(self.map_data_type(value))

        column_clause = ', '.join(list_columns)
        value_clause = ', '.join(list_values)

        query = f"INSERT INTO {self.database}.{table} ({column_clause}) VALUES ({value_clause})"

        logger.info(f"Executing INSERT query: {query}")
        self.execute_postgresql_dml_query(query)
        logger.info(f"INSERT query executed successfully")

    def update_sql(self, table: str, set_values: dict, filter_conditions: dict) -> str:
        query = f"""
            UPDATE {self.database}.{table}
            SET {self.build_set_clause(set_values)}
            WHERE 1=1
            {self.build_where_clause(filter_conditions)}
        """

        logger.info(f"Executing UPDATE query: {query}")
        self.execute_postgresql_dml_query(query)
        logger.info(f"UPDATE query executed successfully")

    def append_to_postgresql(self, table: str, dataframe: DataFrame) -> None:
        """
        Append a DataFrame to a PostgreSQL table
        """
        connection_details = {
            'user': self.user,
            'password': self.password
        }

        logger.info(f"Writing data to PostgreSQL for table \"{self.database}.{table}\"")

        try:
            dataframe.write.jdbc(
                url=self.postgresql_jdbc_url,
                table=table,
                mode='append',
                properties=connection_details
            )

            logger.info(f"Data written to PostgreSQL successfully for table \"{self.database}.{table}\"")
        except Exception as e:
            logger.exception(f"Error writing data to PostgreSQL for table \"{self.database}.{table}\": {e}")
