MONGODB_CONFIG = {
    'host':'mongodb://somehost1-1.test,somehost2-2.test,somehost3-3.test,somehost4-4.test'
}

CLICKHOUSE_CONFIG_RO = {
    'user': 'someuser',
    'host': 'somehost.test',
    'password': 'somepassword'
}
CLICKHOUSE_CONFIG_RW = {
    'user': 'someuser',
    'host': 'somehost.test',
    'password': 'somepassword'
}
MINIO_CONFIG = {
    'endpoint': 'someendpoint',
    'access_key': 'somepassword',
    'secret_key': 'somesecretkey',
    'bucket': 'somebucket',
    'prefix':'someprefix'
}
API_DATA = 'somedata'
QUEUE = MONGODB_CONFIG['host']
