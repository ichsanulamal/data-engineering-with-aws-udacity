from db_utils import get_db_connection, execute_queries, logger
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    execute_queries(cur, conn, drop_table_queries, "Dropping table")


def create_tables(cur, conn):
    execute_queries(cur, conn, create_table_queries, "Creating table")


def main():
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                drop_tables(cur, conn)
                create_tables(cur, conn)
                conn.commit()
        logger.info("Table creation completed")
    except Exception as e:
        logger.error(f"Table creation failed: {e}")
        raise


if __name__ == "__main__":
    main()
