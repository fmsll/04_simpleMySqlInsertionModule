import requests
import logging
import mysql

# DEFINE LOGGING PARAMETERS
logging.basicConfig(filename="imunizalog.log",
                    filemode='w',
                    format=":%(levelname)s: %(message)s :%(asctime)s;",
                    level=logging.INFO)