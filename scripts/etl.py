import configparser
from psycopg2 import connect
from psycopg2.extensions import connection, cursor
from scripts.sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur: cursor, conn: connection) -> None:
    """
    Loads data from S3 to staging tables

    :param cur: DB Cursor
    :param conn: DB Connection
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur: cursor, conn: connection) -> None:
    """
    Executes insert statements to transfer data from staging tables to schema tables

    :param cur: DB Cursor
    :param conn: DB Connection
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main() -> None:
    config = configparser.ConfigParser()
    config_file_name = "dwh.cfg"
    config.read(config_file_name)

    conn = connect(
        f"host={config['CLUSTER']['HOST']} "
        f"dbname={config['CLUSTER']['DB_NAME']} "
        f"user={config['CLUSTER']['DB_USER']} "
        f"password={config['CLUSTER']['DB_PASSWORD']} "
        f"port={config['CLUSTER']['DB_PORT']}"
    )
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
