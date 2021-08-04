from typing import Optional
import logging
import json

import mysql.connector

# DEFINE LOGGING PARAMETERS
logging.basicConfig(filename="imunizalog.log",
                    filemode='w',
                    format=":%(levelname)s: %(message)s :%(asctime)s;",
                    level=logging.INFO)

# dEFINE GLOBAL VARIABLES
#
# mysqldb: Global variable for mysql connection instance
# db_credentials_file: variable to indicate the mysql credentials json file
mysqldb: Optional[mysql.connector.connection_cext.CMySQLConnection] = None
mycursor: Optional[mysql.connector.connection_cext.CMySQLCursor] = None
db_credentials_file = "mysql_credentials.json"

# dEFINE MYSQL CONNECTION PARAMETERS
#
# db_hostname : str
#   Hostname or ip Address from mysql server
# db_username : str
#   Username to access mysql server
# db_password : str
#   Password to access mysql server
db_hostname: Optional[str] = None
db_username: Optional[str] = None
db_password: Optional[str] = None
db_name: Optional[str] = None


def connect_mysql(db_hostname: Optional[str],
                  db_username: Optional[str],
                  db_password: Optional[str],
                  db_name: Optional[str]
                  ) -> bool:
    """tRY TO CONNECT WITH SQL DATABASE
    """
    global mysqldb
    try:
        logging.info("Trying connect to MySql")
        mysqldb = mysql.connector.connect(
            host=db_hostname,
            user=db_username,
            password=db_password,
            database=db_name
        )
    except mysql.connector.errors.Error as ex:
        logging.warning("Connection try with MySQL has failed: " + ex.msg)
        return False

    if mysqldb:
        logging.info("Connection established with MySQL")
        return True


def get_mysql_credentials(credentials_file: str) -> dict:
    with open(credentials_file) as file:
        load_credentials = json.load(file)
        return load_credentials


def set_mysql_cursor() -> bool:
    global mysqldb
    global mycursor
    try:
        mycursor = mysqldb.cursor()
    except AttributeError as error:
        logging.warning("Set cursor failed: " + str(error))
        return False

    if mycursor:
        logging.info("Cursor set successful")
        return True


if __name__ == "__main__":
    credentials = get_mysql_credentials(db_credentials_file)
    print(set_mysql_cursor())
    print(connect_mysql(credentials['db_hostname'],
                        credentials['db_username'],
                        credentials['db_password'],
                        credentials['db_name']))
    print(set_mysql_cursor())