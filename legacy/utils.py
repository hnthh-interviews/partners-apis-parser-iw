import requests
import json
import time
from .config import API_DATA, CLICKHOUSE_CONFIG_PROD
from clickhouse_driver import Client
from common.utils import safe_clickhouse
import pandas as pd

def read_clickhouse(query: str, client=None, raw = False, **kwargs) -> pd.DataFrame:
    if isinstance(client, str):
        client = Client(client)
    else:
        client = Client(**CLICKHOUSE_CONFIG_PROD) if client is None else client

    result, columns = safe_clickhouse(client.execute, query, with_column_types=True)
    if raw:
        return result
    else:
        return pd.DataFrame(result, columns=[x[0] for x in columns], **kwargs)
