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

    def create_table(self, table_name: str, table_info: list[str]) -> None:
        try:
            query: str = (
                f"drop table if exists {table_name}; create table if not exists {table_name} ({", ".join(table_info)})"
            )
            self.run_query(query)
            self.database_info[table_name] = table_info
        except Exception as error:
            print(f"Error creating table '{table_name}':\n\t", error)

    def create_tables(
        self, table_dict: dict[str, list[str]], data_paths: dict[str, list[str]] = None
    ) -> None:
        for table_name, table_info in table_dict.items():
            self.create_table(table_name, table_info)

        if data_paths is None:
            return

        for table_name in table_dict:
            if table_name not in table_dict:
                continue
            self.insert_data(table_name, path.join(*data_paths[table_name]))

    def insert_data(self, table_name: str, csv_path: str) -> None:
        columns = self.columns(table_name)
        with open(csv_path, "r") as csv_file:
            csv_reader = csv.reader(csv_file)
            list_of_csv = list(csv_reader)

        headers: list[str] = list_of_csv[0]
        data: list[list[str]] = list_of_csv[1:]

        print(
            f"Mapping {csv_path} to {table_name} with following conventions:\n\t{"\n\t".join([f"{csv_column} -> {table_column}" for csv_column, table_column in zip(headers, columns)])}"
        )
        query: str = (
            f"insert into {table_name} ({", ".join(columns)}) values ({", ".join(["%s" for _ in columns])})"
        )
        self.run_many_queries(query, data)

    def columns(self, table_name: str) -> list[str]:
        """
        Returns column names of a desired table.
        Note: column names CANNOT contain whitespace.
        """
        if table_name not in self.database_info:
            print(f"Table {table_name} does not exist.")
            return []
        return [
            column_info.split()[0] for column_info in self.database_info[table_name]
        ]
