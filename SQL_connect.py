from environs import env
import mysql.connector as sql
import csv
from os import path


class ConnectSQL:
    env_dict: dict[str, str]
    database_info: dict[str, list[str]]

    def __init__(self, env_key: str, reset_database: bool = False) -> None:
        self.env_dict = env.dict(env_key)
        self.database_info = {}

        if reset_database:
            self.reset_database()
        else:
            self.create_connection(True)

    def close_all(self) -> None:
        self.cursor.close()
        self.connection.close()

    def create_connection(self, to_database: bool) -> None:
        try:
            if to_database:
                self.connection = sql.connect(**self.env_dict)
            else:
                self.connection = sql.connect(
                    user=self.env_dict["user"],
                    password=self.env_dict["password"],
                    host=self.env_dict["host"],
                    port=self.env_dict["port"],
                )
            self.create_cursor()

        except Exception as error:
            print(f"Error creating connection:\n\t", error)

    def create_cursor(self) -> None:
        try:
            self.cursor = self.connection.cursor()
        except Exception as error:
            print(f"Error creating cursor:\n\t", error)

    def commit(self) -> None:
        self.connection.commit()

    def run_query(self, query: str, auto_commit: bool = True) -> None:
        try:
            # Splits at ";" since the cursor can't handle multiple queries in one string.
            for query in query.split(";"):
                self.cursor.execute(query)

            if not auto_commit:
                return
            self.commit()
        except Exception as error:
            print(f"Error executing query '{query}':\n\t", error)

    def run_many_queries(
        self, query: str, data: list[list[str]], auto_commit: bool = True
    ) -> None:
        try:
            self.cursor.executemany(query, data)
            if not auto_commit:
                return
            self.commit()
        except Exception as error:
            print(f"Error executing queries '{query}':\n\t", error)

    def reset_database(self) -> None:
        self.create_connection(False)
        self.run_query(
            f"drop database if exists {self.env_dict["database"]}; create database if not exists {self.env_dict["database"]};"
        )
        self.create_connection(True)
