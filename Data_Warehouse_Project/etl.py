import configparser
import psycopg2
from psycopg2 import OperationalError
from sql_queries import copy_table_queries, insert_table_queries


def load_config(path="dwh.cfg"):
    """Load configuration from the provided path."""
    config = configparser.ConfigParser()
    config.read(path)
    return config["CLUSTER"]


def connect_to_redshift(config):
    """Establish a connection to the Redshift cluster."""
    try:
        conn = psycopg2.connect(
            host=config["HOST"],
            dbname=config["DB_NAME"],
            user=config["DB_USER"],
            password=config["DB_PASSWORD"],
            port=config["DB_PORT"],
        )
        return conn
    except OperationalError as e:
        print(f"Error connecting to the database: {e}")
        raise


def load_staging_tables(cur):
    """Run COPY commands to load data into staging tables."""
    for query in copy_table_queries:
        try:
            cur.execute(query)
        except Exception as e:
            print(f"Error executing COPY command: {e}")


def insert_tables(cur):
    """Run INSERT commands to transform data into analytics tables."""
    for query in insert_table_queries:
        try:
            cur.execute(query)
        except Exception as e:
            print(f"Error executing INSERT command: {e}")


def main():
    config = load_config()
    try:
        with connect_to_redshift(config) as conn:
            with conn.cursor() as cur:
                load_staging_tables(cur)
                insert_tables(cur)
                conn.commit()
                print("Staging tables loaded and data inserted successfully.")
    except Exception as e:
        print(f"ETL process failed: {e}")


if __name__ == "__main__":
    main()
