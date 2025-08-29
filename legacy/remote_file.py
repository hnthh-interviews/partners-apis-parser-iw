from .config import REMOTE_FS, ROOT
from minio import Minio
import pickle
import os.path

def ensure(file):
    local = f'{ROOT}/{file}'
    if os.path.exists(local):
        return local
    m = Minio(REMOTE_FS['endpoint'],
              access_key=REMOTE_FS['access_key'],
              secret_key=REMOTE_FS['secret_key'],
              secure=False)
    m.fget_object(REMOTE_FS['bucket'], file, local)
    return local

def load_pickle_from_minio(file):
    local = ensure(file)
    
    with open(local,'rb') as f:
        return pickle.load(f)
