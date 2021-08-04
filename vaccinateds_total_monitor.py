from typing import Optional
import logging

import mysql.connector

# DEFINE LOGGING PARAMETERS
logging.basicConfig(filename="imunizalog.log",
                    filemode='w',
                    format=":%(levelname)s: %(message)s :%(asctime)s;",
                    level=logging.INFO)

# dEFINE GLOBAL VARIABLES
mysqldb: Optional[mysql.connector.connection_cext.CMySQLConnection] = None

# dEFINE MYSQL CONNECTION PARAMETERS
#
# pARAMETERS
# ----------
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


if __name__ == "__main__":
    pass
