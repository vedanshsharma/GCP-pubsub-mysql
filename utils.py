from sqlalchemy import create_engine, sql, text
import os
from dotenv import load_dotenv

load_dotenv()
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_HOST = os.getenv("MYSQL_HOST")


class dbcon:
    def __init__(self) -> None:
        # creds
        self.db_name = MYSQL_DATABASE
        self.username = MYSQL_USER
        self.password = MYSQL_PASSWORD
        self.host = MYSQL_HOST
        print(MYSQL_DATABASE, MYSQL_USER)
        self.connection = None
        self.cursor = None

    def connect(self):
        try:
            self.engine = create_engine(
                "mysql+mysqlconnector://{}:{}@{}/{}".format(
                    self.username, self.password, self.host, self.db_name
                )
            )
            # self.cursor = self.connection.cursor()
            print("Connected to MySQL database successfully!")
        except Exception as err:
            print(f"Error connecting to database: {err}")
            raise Exception

    def close(self):
        if self.connection:
            try:
                self.connection.close()
                self.cursor = None
                print("Connection to MySQL database closed.")
            except Exception as err:
                print(f"Error closing connection: {err}")

    def execute_query(self, query) -> list:
        result_set = None
        try:
            with self.engine.connect() as connection:
                result_set = connection.execute(text(query))
            return result_set.fetchall()
        except Exception as err:
            print(f"Error executing query: {err}")
