import configparser
import psycopg2
from psycopg2 import sql, OperationalError
from sql_queries import create_table_queries, drop_table_queries


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


def drop_tables(cur):
    """Drop existing tables using predefined SQL queries."""
    for query in drop_table_queries:
        try:
            cur.execute(query)
        except Exception as e:
            print(f"Error dropping table: {e}")


def create_tables(cur):
    """Create tables using predefined SQL queries."""
    for query in create_table_queries:
        try:
            cur.execute(query)
        except Exception as e:
            print(f"Error creating table: {e}")


def main():
    config = load_config()
    try:
        with connect_to_redshift(config) as conn:
            with conn.cursor() as cur:
                drop_tables(cur)
                create_tables(cur)
                conn.commit()
                print("Tables dropped and created successfully.")
    except Exception as e:
        print(f"Process failed: {e}")


if __name__ == "__main__":
    main()
