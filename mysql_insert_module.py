import time
from typing import Optional
import logging
import json

import mysql.connector

# DEFINE LOGGING
logging.basicConfig(filename="logging.log",
                    filemode='w',
                    format=":%(levelname)s: %(message)s :%(asctime)s;",
                    level=logging.INFO)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
console.setFormatter(logging.Formatter(":%(levelname)s: %(message)s :%(asctime)s;"))
logging.getLogger().addHandler(console)

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


def connect_mysql(hostname: Optional[str],
                  username: Optional[str],
                  password: Optional[str],
                  name: Optional[str]
                  ) -> bool:
    """tRY TO CONNECT WITH SQL DATABASE
    """
    global mysqldb
    try:
        logging.info("Trying connect to MySql")
        mysqldb = mysql.connector.connect(
            host=hostname,
            user=username,
            password=password,
            database=name
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


def validate_number_columns_and_values(columns: list, values: list):
    """Validates if the number of values and columns is the same to don't have error when execute
    INSERT function in mysql

    :param columns: Columns that will be affected
    :type columns: list
    :param values: Values for columns
    :type values: list
    :return: True if the number is the same or False if not
    :rtype: bool
    """
    logging.info("Validating if number of columns and values is the same")
    if len(columns) == len(values):
        logging.info("Validation OK")
        return True
    else:
        logging.warning("NUMBER OF COLUMNS AND VALUES ARE DIFFERENT")
        return False


def prepare_statement_for_mysql(table: str, columns: list, values: list) -> Optional[str]:
    string_values = ""
    logging.info("Prepering statement for MySQL")
    if validate_number_columns_and_values(columns, values):
        col = ', '.join(columns)
        number_values = len(values)
        while number_values > 0:
            if number_values == 1:
                string_values += "%s"
                number_values += -1
            else:
                string_values += "%s, "
                number_values += -1
        statement = f'INSERT INTO {table} ({col}) VALUES ({string_values})'
        logging.info("Statement OK")
        return statement
    else:
        return None


def execute_mysql_insert(table: str, columns: list, values: list) -> bool:
    global mysqldb
    global mycursor
    try:
        logging.info("Trying commit in MySQL")
        statement = prepare_statement_for_mysql(table, columns, values)
        c_values = (*values, )
        if statement:
            mycursor.execute(statement, c_values)
            mysqldb.commit()
            logging.info("Commit OK")
            return True
        else:
            raise Exception()
    except mysql.connector.errors.Error as error:
        logging.warning("COMMIT ERROR: " + error.msg)
        return False
    except:
        logging.warning("COMMIT FAILED: ")


if __name__ == "__main__":
    # pass
    credentials = get_mysql_credentials(db_credentials_file)
    if connect_mysql(credentials['db_hostname'],
                     credentials['db_username'],
                     credentials['db_password'],
                     credentials['db_name']):
        if set_mysql_cursor():
            execute_mysql_insert("totalCasos", ["timestamp", "total_vacinados"], [time.strftime('%Y-%m-%d %H:%M:%S'),
                                                                                  "12"])
