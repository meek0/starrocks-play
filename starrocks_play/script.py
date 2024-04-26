from dotenv import dotenv_values
import logging
from mysql.connector import connection, Error
import os
import random
import time

# Set up logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

# Also log to a file
file_handler = logging.FileHandler("play.log")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

env_config = dotenv_values(".env")

MYSQL_CONFIG = {
    'user': env_config.get("STARROCKS_USER"),
    'password': env_config.get("STARROCKS_PASSWD"),
    'host': env_config.get("STARROCKS_HOST"),
    'port': env_config.get("STARROCKS_PORT"),
    'database': env_config.get("STARROCKS_DB")
}

def connect_starrocks(config, attempts=3, delay=2):
    attempt = 1
    # Implement a reconnection routine
    while attempt < attempts + 1:
        try:
            return connection.MySQLConnection(**config)
        except (Error, IOError) as err:
            if (attempts is attempt):
                # Attempts to reconnect failed; returning None
                logger.info("Failed to connect, exiting without a connection: %s", err)
                return None
            logger.info(
                "Connection failed: %s. Retrying (%d/%d)...",
                err,
                attempt,
                attempts-1,
            )
            # progressive reconnect delay
            time.sleep(delay ** attempt)
            attempt += 1
    return None

def get_a_query_from(directory, ids_file):
    if (os.path.isdir(directory) and os.path.isfile(ids_file)):
        i = open(ids_file, "r")
        ids = i.read().splitlines()
        i.close()

        random_file = random.choice(os.listdir(directory))

        f = open(os.path.join(directory, random_file), "r")
        query = f.read()
        f.close()

        return (query, random.choice(ids))
    else:
        logger.error("'%s' and/or '%s' not valid", directory, ids_file)


def start():

    cnx = connect_starrocks(MYSQL_CONFIG)

    if cnx and cnx.is_connected():

        for i in range(0, int(env_config.get("NUMBER_OF_LOOPS", "10"))):
            (QUERY, SAMPLE_ID) = get_a_query_from(env_config.get("SQL_SCRIPTS_DIR"), env_config.get("IDS_FILE"))

            cursor = cnx.cursor()
            logger.info("Starting query execution: '%s' with '%s'", QUERY, SAMPLE_ID)
            start_ns = time.process_time_ns()

            result = cursor.execute(QUERY, (SAMPLE_ID,))
            rows = cursor.fetchall()

            logger.info("Query execution ended in %dms", ((time.process_time_ns() - start_ns) / 1000))

            # for rows in rows:
            #     print(rows)

        logger.info("-----")
        cursor.close()
        cnx.close()
    else:
        print("Could not connect")

if __name__ == "__main__":
    start()