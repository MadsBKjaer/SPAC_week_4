from environs import env
import csv
from os import path

import mysql.connector as sql
import mysql.connector.cursor as sql_cursor


class ConnectSQL:
    connection: sql.MySQLConnection | None
    cursor: sql_cursor.MySQLCursor | None
    env_key: str
    # database_info: dict[str, list[str]]

    def __init__(
        self,
        env_key: str | None = None,
        database: str | None = None,
        create_database: bool = False,
    ) -> None:
        """
        Initializes ConnectSQL class.
        If env_key is provided retrieves connection info from .env file.
        .env file should include of following shape:
            env_key = "user=_____,password=_____,host=_____,port=_____"
        If a database is provided an attempt is made to connect the database.
        create_database: Create database if it does not exist, default False.

        >>> database = ConnectSQL()
        >>> database = ConnectSQL(database = "new_database1")
        >>> database = ConnectSQL("localhost")
        >>> database.create_database("new_database1")
        >>> database = ConnectSQL("localhost", "new_database1")
        >>> database = ConnectSQL("localhost", "new_database2")
        Error selecting database: 1049 (42000): Unknown database 'new_database2'
        >>> database = ConnectSQL("localhost", "new_database2", create_database = True)
        >>> database.drop_database("new_database1")
        >>> database.drop_database("new_database2")
        >>> database.close_all()
        """
        self.connection = None
        self.cursor = None
        if env_key is None:
            self.env_key = None
            return
        self.env_key = env_key
        self.connect()
        self.create_cursor()

        if not database:
            return
        if create_database:
            self.create_database(database)
            return
        self.use_database(database)

    def connect(self, connection_args: dict[str, str] | None = None) -> None:
        """
        Creates connection.
        The env_key is used if connection_args is not provided.

        >>> database = ConnectSQL()
        >>> database.connect({"user" : "root", "password" : "250202", "host" : "localhost", "port" : "3306"})
        >>> database.close_all()
        >>> database.connect({"user" : "root", "host" : "localhost", "port" : "3306"})
        Error creating connection: 1045 (28000): Access denied for user 'root'@'localhost' (using password: NO)
        >>> database.close_all()
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

        >>> database = ConnectSQL()
        >>> database.connect({"user" : "root", "password" : "250202", "host" : "localhost", "port" : "3306"})
        >>> database.create_cursor()
        >>> database.close_all()
        >>> database = ConnectSQL()
        >>> database.create_cursor()
        Error creating cursor: 'NoneType' object has no attribute 'cursor'
        >>> database.close_all()
        """
        try:
            self.cursor = self.connection.cursor()
        except Exception as e:
            print(f"Error creating cursor:", e)

    def close_all(self) -> None:
        """
        Closes both cursor and connection.

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
        >>> ("new_database",) in database.cursor.fetchall()
        True
        >>> database.drop_database("new_database")
        >>> database.close_all()
        >>> database = ConnectSQL("localhost")
        >>> database.create_database("new_database", use = False)
        >>> database.cursor.execute("show databases")
        >>> ("new_database",) in database.cursor.fetchall()
        True
        >>> database.drop_database("new_database")
        >>> database.close_all()
        """
        try:
            if overwrite:
                self.cursor.execute(f"drop database if exists {database}")
            self.cursor.execute(f"create database if not exists {database}")
        except Exception as e:
            print(f"Error creating database:", e)

        if use:
            self.use_database(database)

    def use_database(self, database: str) -> None:
        """
        Selects database to use.

        >>> database = ConnectSQL("localhost")
        >>> database.create_database("new_database", use = False)
        >>> database.use_database("new_database")
        >>> database.cursor.execute("select database()")
        >>> ("new_database",) in database.cursor.fetchall()
        True
        >>> database.drop_database("new_database")
        >>> database.use_database("unknown_database")
        Error selecting database: 1049 (42000): Unknown database 'unknown_database'
        >>> database.close_all()
        """
        try:
            self.cursor.execute(f"use {database}")
        except Exception as e:
            print(f"Error selecting database:", e)

    def create_table(
        self, table: str, table_info: list[str], overwrite: bool = False
    ) -> None:
        """
        Takes a table name and table info and creates a table.
        Table info should be a list of string providing datatype and additional attributes.
        Template: ['column_name data_type attributes', 'column_name2 data_type2 attributes', ...]
        overwrite controls if any existing table with the same name should be overwritten.

        >>> database = ConnectSQL("localhost")
        >>> database.create_database("new_database")
        >>> database.create_table("new_table", ["id tinyint unique"])
        >>> database.cursor.execute("show tables")
        >>> tables = database.cursor.fetchall()
        >>> ("new_table",) in tables
        True
        >>> database.drop_database("new_database")
        >>> database.close_all()
        """
        try:
            if overwrite:
                self.cursor.execute(f"drop table if exists {table}")
            self.cursor.execute(
                f"create table if not exists {table} ({", ".join(table_info)})"
            )
        except Exception as e:
            print(f"Error creating table:", e)

    def commit(self) -> None:
        """
        Commits database changes.
        """
        try:
            self.connection.commit()
        except Exception as e:
            print(f"Error committing:", e)

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

    def insert_data(
        self,
        table: str,
        data: str | list[list[str]],
        table_columns: list[str] | None = None,
        data_columns: list[str] | None = None,
        auto_commit: bool = True,
    ) -> None:
        """
        Inserts data into table.
        Accepts data as path to csv file or list of list of values i.e. [[value11, value12], [value21, value22]].
        Accepts table columns as a way to control order of columns. Defaults to the order of columns that the table provides if unprovided.
        Accepts data columns which is used for printing source and destination columns (debugging tool).
        If a csv path is provided the input is ignored and column names from the csv file is used.

        >>> database = ConnectSQL("localhost")
        >>> database.create_database("new_database")
        >>> database.create_table("new_table", ["first_name varchar(255)", "last_name varchar(255)"])
        >>> database.insert_data("new_table", [["John", "Smith"], ["Karen", "Johnson"]])
        >>> database.close_all()
        >>> database = ConnectSQL("localhost", "new_database")
        >>> database.cursor.execute("select * from new_table")
        >>> database.cursor.fetchall()
        [('John', 'Smith'), ('Karen', 'Johnson')]
        >>> database.create_table("new_table", ["id tinyint", "name varchar(255)", "price float(15, 5)"], overwrite = True)
        >>> database.insert_data("new_table", path.join("data", "products.csv"), auto_commit = False)
        Mapping columns with following conventions: id -> id, name -> name, price -> price
        >>> database.cursor.execute("select * from new_table where id = 2")
        >>> database.cursor.fetchall()
        [(2, 'Tablet', 490.03461)]
        >>> database.close_all()
        >>> database = ConnectSQL("localhost", "new_database")
        >>> database.cursor.execute("select * from new_table where id = 2")
        >>> database.cursor.fetchall()
        []
        >>> database.drop_database("new_database")
        >>> database.close_all()
        """

        if table_columns is None:
            table_columns = self.columns(table)

        if type(data) is str:
            data, data_columns = self.read_csv(data)

        if data_columns is not None:
            print(
                f"Mapping columns with following conventions: {", ".join([f"{data_column} -> {table_column}" for data_column, table_column in zip(data_columns, table_columns)])}"
            )

        try:
            self.cursor.executemany(
                f"insert into {table} ({", ".join(table_columns)}) values ({", ".join(["%s" for _ in table_columns])})",
                data,
            )
        except Exception as e:
            print(f"Error inserting data:", e)

        if auto_commit:
            self.commit()

    def read_csv(self, csv_path: str) -> tuple[list[list[str]], list[str]]:
        """
        Reads csv file from path and returns (data, column names)

        >>> database = ConnectSQL()
        >>> data, columns = database.read_csv(path.join("data", "products.csv"))
        >>> data[2]
        ['2', 'Tablet', '490.0346']
        >>> columns
        ['id', 'name', 'price']
        """
        try:
            with open(csv_path, "r") as csv_file:
                csv_reader = csv.reader(csv_file)
                list_of_csv = list(csv_reader)
            return list_of_csv[1:], list_of_csv[0]
        except Exception as e:
            print(f"Error reading csv file:", e)

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

        >>> database = ConnectSQL("localhost")
        >>> database.create_database("new_database")
        >>> database.create_table("new_table", ["first_name varchar(255)", "last_name varchar(255)"])
        >>> database.columns("new_table")
        ['first_name', 'last_name']
        >>> database.drop_database("new_database")
        >>> database.close_all()
        """
        try:
            self.cursor.execute(f"show columns from {table}")
        except Exception as e:
            print(f"Error getting columns:", e)

        columns = self.cursor.fetchall()
        return [column[0] for column in columns]

    def tables(self) -> list[str]:
        """
        Shows tables in current database.

        >>> database = ConnectSQL("localhost")
        >>> database.create_database("new_database")
        >>> database.create_table("new_table1", ["first_name varchar(255)", "last_name varchar(255)"])
        >>> database.create_table("new_table2", ["first_name varchar(255)", "last_name varchar(255)"])
        >>> database.tables()
        ['new_table1', 'new_table2']
        >>> database.drop_database("new_database")
        >>> database.close_all()
        """
        try:
            self.cursor.execute("show tables")
        except Exception as e:
            print(f"Error getting tables:", e)

        tables = self.cursor.fetchall()
        return [table[0] for table in tables]

    def select(self, table: str, columns: list[str] | str | None = None) -> None:
        """
        Takes a table and columns and returns given columns from desired table as list.
        To select all columns let the argument columns be None or "*" (default behavior).
        To select multiple columns provide a list of column names.
        A single column can be provided as a string.

        >>> database = ConnectSQL("localhost")
        >>> database.create_database("new_database")
        >>> database.create_table("new_table", ["first_name varchar(255)", "last_name varchar(255)", "country varchar(255)"])
        >>> database.insert_data("new_table", [["John", "Smith", "U.K."], ["Karen", "Johnson", "CAreNADA"], ["John", "Cena", "USA"]])
        >>> database.select("new_table")
        >>> database.cursor.fetchall()
        [('John', 'Smith', 'U.K.'), ('Karen', 'Johnson', 'CAreNADA'), ('John', 'Cena', 'USA')]
        >>> database.select("new_table", ["last_name", "country"])
        >>> database.cursor.fetchall()
        [('Smith', 'U.K.'), ('Johnson', 'CAreNADA'), ('Cena', 'USA')]
        >>> database.select("new_table", "first_name")
        >>> database.cursor.fetchall()
        [('John',), ('Karen',), ('John',)]
        >>> database.drop_database("new_database")
        >>> database.close_all()
        """

        if columns is None:
            columns = "*"
        elif type(columns) is list:
            columns = ", ".join(columns)

        try:
            self.cursor.execute(f"select {columns} from {table}")
        except Exception as e:
            print(f"Error selecting:", e)

    def update(
        self,
        table: str,
        update_list: list[tuple[str]],
        conditions: list[tuple[str]],
        auto_commit: bool = True,
    ) -> None:
        if table not in self.tables():
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
        if table not in self.tables():
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
        if primary_table not in self.tables():
            print(f"Table {primary_table} does not exist.")
            return
        primary_query: str = (
            f"alter table {primary_table} add primary key ({primary_column})"
        )
        self.run_query(primary_query)

        if foreign_table is None:
            return

        if foreign_table not in self.tables():
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
        for table in tables:
            if table not in self.tables():
                print(f"Table {table} does not exist.")
                return

        query: str = f"{tables[0]} "
        for i, table in enumerate(tables[1:]):
            query += f"{join_type} join {table} on {tables[0]}.{columns[i]} = {table}.{columns[i]} "

        return query

    def drop_table(self, database: str) -> None:
        """
        Drops table.

        >>> database = ConnectSQL("localhost")
        >>> database.create_database("new_database")
        >>> database.create_table("new_table", ["id tinyint"])
        >>> database.cursor.execute("show tables")
        >>> ("new_table",) in database.cursor.fetchall()
        True
        >>> database.drop_table("new_table")
        >>> database.cursor.execute("show tables")
        >>> ("new_table",) in database.cursor.fetchall()
        False
        """
        try:
            self.cursor.execute(f"drop table if exists {database}")
        except Exception as e:
            print(f"Error dropping table:", e)

    def drop_database(self, database: str) -> None:
        """
        Drops database.

        >>> database = ConnectSQL("localhost")
        >>> database.create_database("new_database")
        >>> database.cursor.execute("show databases")
        >>> ("new_database",) in database.cursor.fetchall()
        True
        >>> database.drop_database("new_database")
        >>> database.cursor.execute("show databases")
        >>> ("new_database",) in database.cursor.fetchall()
        False
        """
        try:
            self.cursor.execute(f"drop database if exists {database}")
        except Exception as e:
            print(f"Error dropping database:", e)
