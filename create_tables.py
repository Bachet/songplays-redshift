import boto3
import configparser
import logging
from psycopg2 import connect
from psycopg2.extensions import connection, cursor
from sql_queries import create_table_queries, drop_table_queries
from sql_queries import create_staging_table_queries, drop_staging_table_queries


def drop_tables(cur: cursor, conn: connection) -> None:
    """
    Drops all tables

    :param cur: DB Cursor
    :param conn: DB Connection
    """
    for query in drop_staging_table_queries:
        cur.execute(query)
        conn.commit()

    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur: cursor, conn: connection) -> None:
    """
    Creates all tables

    :param cur: DB Cursor
    :param conn: DB Connection
    """
    for query in create_staging_table_queries:
        cur.execute(query)
        conn.commit()

    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main() -> None:
    config = configparser.ConfigParser()
    config_file_name = "dwh.cfg"
    config.read(config_file_name)

    redshift = boto3.client(
        "redshift",
        region_name=config["CLUSTER"]["REGION"],
        aws_access_key_id=config["CREDENTIALS"]["KEY"],
        aws_secret_access_key=config["CREDENTIALS"]["SECRET"],
    )
    try:
        cluster_props = redshift.describe_clusters(ClusterIdentifier="dwhCluster")["Clusters"][0]
        host = cluster_props["Endpoint"]["Address"]
        config.set("CLUSTER", "HOST", host)
        with open(config_file_name, 'w') as configfile:
            config.write(configfile)
    except Exception as e:
        logging.error("cluster is not available", exc_info=e)

    conn = connect(
        f"host={config['CLUSTER']['HOST']} "
        f"dbname={config['CLUSTER']['DB_NAME']} "
        f"user={config['CLUSTER']['DB_USER']} "
        f"password={config['CLUSTER']['DB_PASSWORD']} "
        f"port={config['CLUSTER']['DB_PORT']}"
    )
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
