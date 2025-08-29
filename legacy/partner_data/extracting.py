#from ..partners_api import PartnerData
from .data import PartnerData
from dataclasses import asdict
from ..config import CLICKHOUSE_CONFIG_PROD, RECIEVER_APP_PASSWORD, RECIEVER_EMAIL
from .email_wrapper import EmailWrapper
from clickhouse_driver import Client
import pandas as pd
from io import BytesIO

def insert_dataframe(df: pd.DataFrame, ssp: str, currency = 'usd'):
    data = []
    client = Client(CLICKHOUSE_CONFIG_PROD['host'])
    for _, row in df.iterrows():
        d = PartnerData(
            date=row['date'],
            ssp=ssp,
            imps=int(row['imps']),
            spent=float(row['spend']),
            currency=currency
        )
        data.append(d)
    query = "INSERT INTO somedb.partner_data (*) VALUES"
    return client.execute(query, (asdict(d) for d in data))

def read_csv_from_bytes(bytes_object: BytesIO, *args, **kwargs) -> pd.DataFrame:
    bytes_object.seek(0)
    return pd.read_csv(bytes_object, *args, **kwargs)

def read_excel_from_bytes(bytes_object: BytesIO, *args, **kwargs) -> pd.DataFrame:
    bytes_object.seek(0)
    return pd.read_excel(bytes_object, *args, **kwargs)

def get_email_wrapper() -> EmailWrapper:
    return EmailWrapper(RECIEVER_EMAIL, RECIEVER_APP_PASSWORD)
