from SQL_connect import ConnectSQL

tables: dict[str, list[str]] = {}
data_paths: dict[str, list[str]] = {}

data_paths["orders_combined"] = ["data", "orders_combined.csv"]
tables["orders_combined"] = [
    "order_id tinyint unsigned unique",
    "date_time timestamp",
    "customer_name varchar(255)",
    "customer_email varchar(255)",
    "product_name varchar(255)",
    "product_price float(15, 5)",
]

data_paths["customers"] = ["data", "customers.csv"]
tables["customers"] = [
    "customer_id tinyint unsigned unique",
    "customer_name varchar(255)",
    "email varchar(255)",
]

data_paths["orders"] = ["data", "orders.csv"]
tables["orders"] = [
    "order_id tinyint unsigned unique",
    "date_time timestamp",
    "customer_id tinyint unsigned",
    "product_id tinyint unsigned",
]

data_paths["products"] = ["data", "products.csv"]
tables["products"] = [
    "product_id tinyint unsigned unique",
    "product_name varchar(255)",
    "price float(15, 5)",
]


if __name__ == "__main__":
    db_name: str = "tech_store"
    database = ConnectSQL(env_key="tech_store_db", reset_database=True)

    try:
        database.create_tables(tables, data_paths)
        # database.update(
        #     "products",
        #     [("product_name", "Phone"), ("product_price", 10000)],
        #     [("product_name", "=", "Phone")],
        # )
        # database.delete("products", [("product_name", "=", "Phone")])
        # print(database.database_info)
        # print(database.tables())
        database.add_key("orders", "order_id")
        database.add_key("customers", "customer_id", "orders")
        database.add_key("products", "product_id", "orders")
        pass
    except Exception as error:
        print(f"Error running test:", error)
    finally:
        database.close_all()
