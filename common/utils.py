import datetime
from pathlib import Path
import time
import aiohttp
import connexion
from clickhouse_driver.errors import ServerException, NetworkError, SocketTimeoutError, ErrorCodes

import pandas as pd
from minio import Minio
from io import BytesIO
import uuid
from clickhouse_driver import Client
from .config import CLICKHOUSE_CONFIG_RO,CLICKHOUSE_CONFIG_RW,MINIO_CONFIG,MYSQL_CONFIG_PROD,API_DATA
import json
from  mysql.connector import connect
import re
import requests
import pyarrow
import numpy

def safe_clickhouse(f, *args, **kwargs):
    retries = 5
    sleep = 5

    safe_codes = {
        ErrorCodes.UNEXPECTED_END_OF_FILE,
        ErrorCodes.ATTEMPT_TO_READ_AFTER_EOF,
        ErrorCodes.CANNOT_READ_ALL_DATA,
        ErrorCodes.CANNOT_READ_FROM_SOCKET,
        ErrorCodes.CANNOT_WRITE_TO_SOCKET,
        ErrorCodes.TOO_MANY_SIMULTANEOUS_QUERIES,
        ErrorCodes.NO_FREE_CONNECTION,
        ErrorCodes.SOCKET_TIMEOUT,
        ErrorCodes.NETWORK_ERROR,
        ErrorCodes.ABORTED,
        ErrorCodes.MEMORY_LIMIT_EXCEEDED,
        ErrorCodes.QUERY_WAS_CANCELLED
    }
    
    for i in range(5):
        try:
            return f(*args, **kwargs)
        except EOFError as ex:
            print(f'WARNING: Clickhouse ignored EOFError')
        except (ServerException, NetworkError, SocketTimeoutError) as ex:
            if ex.code in safe_codes:
                print(f'WARNING: Clickhouse ignored exception: {ex}')
            else:
                raise
        time.sleep(sleep)

    print(f'Query failed due to too many errors: {args} {kwargs}')
    raise RuntimeError(f'Query failed due to too many errors: {args} {kwargs}')

content_types = {
    '.html': 'text/html',
    '.css': 'text/css',
    '.js': 'text/javascript',
    '.json': 'application/json'
}
