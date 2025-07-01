import configparser
import logging
import psycopg2
from contextlib import contextmanager

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = logging.getLogger(__name__)


@contextmanager
def get_db_connection(config_file="dwh.cfg"):
    """Database connection context manager"""
    config = configparser.ConfigParser()
    config.read(config_file)

    cluster = config["CLUSTER"]
    params = {
        "host": cluster["HOST"],
        "dbname": cluster["DB_NAME"],
        "user": cluster["DB_USER"],
        "password": cluster["DB_PASSWORD"],
        "port": cluster["DB_PORT"],
    }

    conn = None
    try:
        conn = psycopg2.connect(**params)
        logger.info("Connected to database")
        yield conn
    except Exception as e:
        logger.error(f"Database error: {e}")
        raise
    finally:
        if conn:
            conn.close()


def execute_queries(cur, conn, queries, operation):
    """Execute queries with error handling"""
    for i, query in enumerate(queries, 1):
        try:
            logger.info(f"[{i}/{len(queries)}] {operation}")
            cur.execute(query)
        except Exception as e:
            logger.error(f"{operation} failed: {e}")
            raise
