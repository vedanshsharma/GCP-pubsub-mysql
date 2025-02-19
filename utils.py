from sqlalchemy import create_engine, sql
import os
from dotenv import load_dotenv

load_dotenv()
# creds
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_HOST = os.getenv("MYSQL_HOST")


class dbcon:
    def __init__(self, db_name, username, password, host) -> None:
        self.db_name = db_name
        self.username = username
        self.password = password
        self.host = host
        self.connection = None
        self.cursor = None

    def connect(self):
        try:
            self.connection = create_engine(
                "mysql+mysqlconnector://{}:{}@{}/{}".format(
                    self.username, self.password, self.host, self.db_name
                )
            ).raw_connection()
            self.cursor = self.connection.cursor()
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

    def execute_query(self, query, data=None):
        try:
            if data:
                self.cursor.execute(query, data)
            else:
                self.cursor.execute(query)
            self.connection.commit()
        except Exception as err:
            print(f"Error executing query: {err}")
