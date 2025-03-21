from environs import env
import csv
from os import path

import mysql.connector as sql
import mysql.connector.cursor as sql_cursor


class ConnectSQL:
    connection: sql.MySQLConnection | None
    cursor: sql_cursor.MySQLCursor | None
    env_key: str
    database_info: dict[str, list[str]]

    def __init__(self, env_key: str | None = None, database: str | None = None) -> None:
        """
        Initializes ConnectSQL class.
        If env_key is provided retrieves connection info from .env file.
        .env file should include of following shape:
            env_key = "user=_____,password=_____,host=_____,port=_____"
        If a database is provided an attempt is made to connect the database.

        >>> database = ConnectSQL()
        >>> database = ConnectSQL(database = "tech_store")
        >>> database = ConnectSQL("localhost")
        >>> database = ConnectSQL("localhost", "tech_store")
        >>> database = ConnectSQL("localhost", "unknown_database")
        Error selecting database: 1049 (42000): Unknown database 'unknown_database'
        """
        self.connection = None
        self.cursor = None
        if env_key is None:
            self.env_key = None
            return
        self.env_key = env_key
        self.connect()
        self.create_cursor()
        if database:
            self.use_database(database)

    def connect(self, connection_args: dict[str, str] | None = None) -> None:
        """
        Creates connection.
        The env_key is used if connection_args is not provided.

        >>> database = ConnectSQL("localhost")
        >>> database.connect()
        >>> database.connect({"host" : "localhost", "password" : "250202", "user" : "root"})
        >>> database.connect({"host" : "localhost", "user" : "root"})
        Error creating connection: 1045 (28000): Access denied for user 'root'@'localhost' (using password: NO)
        """
        if connection_args is None:
            connection_args = env.dict(self.env_key)
        try:
            self.connection = sql.connect(**connection_args)
        except Exception as e:
            print(f"Error creating connection:", e)

    def create_cursor(self) -> None:
        """
        Creates cursor.

        >>> database = ConnectSQL("localhost")
        >>> database.connect()
        >>> database.create_cursor()
        >>> database = ConnectSQL()
        >>> database.create_cursor()
        Error creating cursor: 'NoneType' object has no attribute 'cursor'
        """
        try:
            self.cursor = self.connection.cursor()
        except Exception as e:
            print(f"Error creating cursor:", e)

    def close_all(self) -> None:
        """
        Closes oth cursor and connection.

        >>> database = ConnectSQL("localhost")
        >>> database.close_all()
        >>> database = ConnectSQL()
        >>> database.close_all()
        """
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

    def create_database(
        self, database: str, use: bool = True, overwrite: bool = False
    ) -> None:
        """
        Creates database.
        Takes a database name, use: whether or not created database should be used right away and if any existing database of same name should be overwritten.

        >>> database = ConnectSQL("localhost")
        >>> database.create_database("new_database")
        >>> database.cursor.execute("select database()")
        >>> database.cursor.fetchall()
        [('new_database',)]
        """
        if overwrite:
            self.cursor.execute(f"drop database if exists {database}")
        self.cursor.execute(f"create database if not exists {database}")

        if use:
            self.use_database(database)

    def use_database(self, database: str) -> None:
        """
        Selects database to use.

        >>> database = ConnectSQL("localhost")
        >>> database.use_database("tech_store")
        >>> database.use_database("unknown_database")
        Error selecting database: 1049 (42000): Unknown database 'unknown_database'
        """
        try:
            self.cursor.execute(f"use {database}")
        except Exception as e:
            print(f"Error selecting database:", e)

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

    def create_table(self, table: str, table_info: list[str]) -> None:
        """
        Takes a table name and table info and creates a table.
        Table info should be a list of string providing datatype and additional attributes.
        Template: ['column_name data_type attributes', 'column_name2 data_type2 attributes', ...]
        """
        try:
            query: str = (
                f"drop table if exists {table}; create table if not exists {table} ({", ".join(table_info)})"
            )
            self.run_query(query)
            self.database_info[table] = table_info
        except Exception as error:
            print(f"Error creating table '{table}':\n\t", error)

    def insert_data(self, table: str, csv_path: str) -> None:
        """
        Takes a table name and a path to a csv file and inserts the data from the csv file into the table.
        Note: does assume that the tables columns are in the same order as in the csv file.
        Prints column names from both table and csv so you can see if the ordering is as expected.
        """
        columns = self.columns(table)
        with open(csv_path, "r") as csv_file:
            csv_reader = csv.reader(csv_file)
            list_of_csv = list(csv_reader)

        headers: list[str] = list_of_csv[0]
        data: list[list[str]] = list_of_csv[1:]

        print(
            f"Mapping {csv_path} to {table} with following conventions:\n\t{"\n\t".join([f"{csv_column} -> {table_column}" for csv_column, table_column in zip(headers, columns)])}"
        )
        query: str = (
            f"insert into {table} ({", ".join(columns)}) values ({", ".join(["%s" for _ in columns])})"
        )
        self.run_many_queries(query, data)

    def create_tables(
        self, table_dict: dict[str, list[str]], data_paths: dict[str, list[str]] = None
    ) -> None:
        for table, table_info in table_dict.items():
            self.create_table(table, table_info)

        if data_paths is None:
            return

        for table in table_dict:
            if table not in table_dict:
                continue
            self.insert_data(table, path.join(*data_paths[table]))

    def columns(self, table: str) -> list[str]:
        """
        Returns column names of a desired table.
        Note: column names CANNOT contain whitespace.
        """
        if table not in self.database_info:
            print(f"Table {table} does not exist.")
            return []
        return [column_info.split()[0] for column_info in self.database_info[table]]

    def tables(self) -> list:
        return self.database_info.keys()

    def select(self, table: str, columns: list[str] | str | None = None) -> list:
        """
        Takes table name and columns and returns given columns from desired table as list.
        To select all columns let the argument columns be None (default behavior).
        To select multiple columns provide a list of column names.
        A single column can be provided as a string.
        """
        # if table not in self.database_info:
        #     print(f"Table {table} does not exist.")
        #     return []

        if columns is None:
            columns = "*"
        elif type(columns) is list:
            columns = ", ".join(columns)

        query: str = f"select {columns} from {table}"
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def update(
        self,
        table: str,
        update_list: list[tuple[str]],
        conditions: list[tuple[str]],
        auto_commit: bool = True,
    ) -> None:
        if table not in self.database_info:
            print(f"Table {table} does not exist.")
            return

        condition_str: str = ", ".join(
            [f"{column} {logic} {repr(value)}" for column, logic, value in conditions]
        )
        update_str: str = ", ".join(
            [f"{column} = {repr(value)}" for column, value in update_list]
        )

        query: str = f"update {table} set {update_str} where {condition_str}"

        self.run_query(query, auto_commit)

    def delete(
        self, table: str, conditions: list[tuple[str]], auto_commit: bool = True
    ) -> None:
        if table not in self.database_info:
            print(f"Table {table} does not exist.")
            return

        condition_str: str = ", ".join(
            [f"{column} {logic} {repr(value)}" for column, logic, value in conditions]
        )

        query: str = f"delete from {table} where {condition_str}"
        self.run_query(query, auto_commit)

    def add_key(
        self,
        primary_table: str,
        primary_column: str,
        foreign_table: str | None = None,
        foreign_column: str | None = None,
    ) -> None:
        """
        Takes a primary table and column where a primary key is added.
        If a foreign table is added a foreign key will be added to the table referencing the primary key.
        If foreign column is not provided it is assumed that the primary and foreign column have the same name, and the primary column is reused.
        """
        if primary_table not in self.database_info:
            print(f"Table {primary_table} does not exist.")
            return
        primary_query: str = (
            f"alter table {primary_table} add primary key ({primary_column})"
        )
        self.run_query(primary_query)

        if foreign_table is None:
            return

        if foreign_table not in self.database_info:
            print(f"Table {foreign_table} does not exist.")
            return

        if foreign_column is None:
            foreign_column = primary_column

        foreign_query: str = (
            f"alter table {foreign_table} "
            f"add foreign key ({foreign_column}) "
            f"references {primary_table}({primary_column})"
        )
        self.run_query(foreign_query)

    def join(self, tables: list[str], join_type: str, columns: list[str]) -> str:
        if not set(tables).issubset(self.database_info):
            print(f"One of tables {tables} does not exist.")
            return

        query: str = f"{tables[0]} "
        for i, table in enumerate(tables[1:]):
            query += f"{join_type} join {table} on {tables[0]}.{columns[i]} = {table}.{columns[i]} "

        return query
