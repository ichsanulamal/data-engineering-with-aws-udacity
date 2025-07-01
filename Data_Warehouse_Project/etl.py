from db_utils import get_db_connection, execute_queries, logger
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    execute_queries(cur, conn, copy_table_queries, "Loading staging")


def insert_tables(cur, conn):
    execute_queries(cur, conn, insert_table_queries, "Inserting data")


def main():
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                load_staging_tables(cur, conn)
                insert_tables(cur, conn)
                conn.commit()
        logger.info("ETL completed")
    except Exception as e:
        logger.error(f"ETL failed: {e}")
        raise


if __name__ == "__main__":
    main()
