import os
from enum import Enum
import pymysql


class LocalTemplateProvider:
    template_folder = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "./sql"
    )


    def get_template(self, template):
        full_sql_path = os.path.join(self.template_folder, template)

        with open(full_sql_path, "r") as sql_file:

            return sql_file.read()


class MysqlConnector:
    def __init__(self, credentials, db_name=None):
        self.connection = pymysql.connect(
            host=credentials["host"],
            user=credentials["username"],
            password=credentials["password"],
            database=credentials["dbname"],

        )

    @classmethod
    def connect(cls, credentials, db_name=None):
        return MysqlConnector(credentials, db_name)

    def cursor(self, as_dict=True):
        return (
            self.connection.cursor(pymysql.cursors.DictCursor)
            if as_dict
            else self.connection.cursor()
        )

    def close(self):
        self.connection.close()

    def commit(self):
        self.connection.commit()


class DBConnection:
    class ReturnType(Enum):
        Tuple = 0
        Dict = 1
        DataFrame = 2

    class SqlRaw:
        def __init__(self, sql_statement, params=()):
            self.query = sql_statement
            self.params = params

        def __call__(self):
            return (self.query, self.params)

    class SqlStringTemplate:
        def __init__(self, sql_statement_template, params):
            self.raw_params = params
            self.raw_query = sql_statement_template

        def __call__(self):
            query, bind_params = self.prepare_query(self.raw_query, self.raw_params)

            return (query, tuple(bind_params))

    class SqlFileTemplate:
        def __init__(self, template_file, params):
            self.raw_query = LocalTemplateProvider().get_template(template_file)
            self.raw_params = params

        def __call__(self):

            return (self.raw_query, self.raw_params)

    def __init__(
            self,
            db_credentials,
            dbname=None,
            retries=1,
    ):
        self.auto_close = True
        self.retries = retries
        self.connection = (
            MysqlConnector.connect(db_credentials, dbname)
        )

    def __enter__(self):
        self.auto_close = False
        return self

    def __exit__(self, exc_type, exc_value, tb):
        self.__close_connection()
        # rethrow the exception if it is available by returning False
        if exc_type is not None:
            return False
        return True

    def __del__(self):
        self.__close_connection()

    def __close_connection(self):
        if self.connection:
            self.connection.close()
            self.connection = None

    def __cursor(self, as_dict=True):
        return self.connection.cursor(as_dict)

    def execute(
            self,
            sql_statement_builder,
            commit=False,
            return_type=ReturnType.Dict,
            no_fetch=False,
            debug=False,
    ):
        attempt = 1
        while attempt <= self.retries:
            (query, bind_params) = sql_statement_builder()
            try:
                as_dict = return_type in [
                    DBConnection.ReturnType.Dict,
                    DBConnection.ReturnType.DataFrame,
                ]

                with self.__cursor(as_dict) as cursor:

                    cursor.execute(query, bind_params)

                    data = None if no_fetch else cursor.fetchall()

                    if commit:
                        self.connection.commit()

                    if self.auto_close:
                        self.__close_connection()

                    return data

            except Exception as e:
                if attempt == self.retries:
                    raise e

            attempt += 1
