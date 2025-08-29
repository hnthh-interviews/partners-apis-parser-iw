import os
import socket

from common.config import *

try:
    if get_ipython().__class__.__name__ == 'ZMQInteractiveShell':
        ROOT = os.path.dirname(os.path.abspath('.'))
    else:
        ROOT = os.getenv('API_ROOT') or os.getenv('HOME')
except:
    if os.getenv('TEST_QUEUE'):
        ROOT = '/tmp'
    else:
        ROOT = os.getenv('API_ROOT') or os.getenv('HOME')
CLICKHOUSE_CONFIG_PROD = CLICKHOUSE_CONFIG_RO
IMAP_HOST = 'verisecret'
IMAP_PORT = 1234

RECEIVER_APP_PASSWORD = 'verisecret'

RECEIVER_EMAIL = 'verisecret@test.com'

QUEUE = {
    'mongo': MONGODB_CONFIG_PROD['host']
}

PARTNER_M_SSP_ACCESS_TOKEN = 'verisecret'

PARTNER_M_DSP_ACCESS_TOKEN ='verisecret'

PARTNER_A_SSP_LOGIN={
    'grant_type': 'verisecret',
    'client_id': 'clientid',
    'username': 'username@test.com',
    'password': 'verisecret'
}

PARTNER_B_DSP_LOGIN_DATA = {'login':'login@test.com',
                          'password':'verisecret',
                          'user_id':12345678}

PARTNER_B_SSP_LOGIN_DATA = {'login':'login@test.com',
                          'password':'verisecret',
                        'user_id':12345678}


PARTNER_S_DSP_TOKEN = 'verisecret'
PARTNER_S_DSP_LOGIN = 'verisecret'

DSP_B_ACCESS_TOKEN = 'verisecret'
