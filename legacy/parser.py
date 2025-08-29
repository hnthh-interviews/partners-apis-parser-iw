from dataclasses import dataclass, asdict
from common.queue import queue
from .config import *
from .import remote_file
import requests
import datetime
import xml.etree.cElementTree as ET
import numpy as np
import time
from minio import Minio
from io import BytesIO
import uuid
import re

from collections import defaultdict
from functools import reduce

from itertools import groupby
from functools import partial
from .remote_file import load_pickle_from_minio

import pandas as pd
import json
from clickhouse_driver import Client
from dataclasses import dataclass
from .utils import read_clickhouse

import xmlrpc.client
import ssl
from itertools import chain

from .partner_data.data import PartnerData2,PartnerData

from .partner_data.partners import insert_recent_partner_s_data

class CookiesTransport(xmlrpc.client.SafeTransport):
    """A SafeTransport (HTTPS) subclass that retains cookies over its lifetime."""

    def __init__(self, context=None):
        super().__init__(context=context)
        self._cookies = []

    def send_headers(self, connection, headers):
        if self._cookies:
            connection.putheader("Cookie", "; ".join(self._cookies))
        super().send_headers(connection, headers)

    def parse_response(self, response):
        if response.msg.get_all("Set-Cookie"):
            for header in response.msg.get_all("Set-Cookie"):
                cookie = header.split(";", 1)[0]
                self._cookies.append(cookie)
        return super().parse_response(response)

def load_insert_data(start_date, finish_date):
    start_date = pd.to_datetime(start_date) if start_date else datetime.date.today()-datetime.timedelta(days=1)
    finish_date = pd.to_datetime(finish_date) if finish_date else datetime.date.today()-datetime.timedelta(days=1)
    data = []
    start_date_str = start_date.strftime("%Y-%m-%d")
    finish_date_str = finish_date.strftime("%Y-%m-%d")

    # ____________________ SSP ______________________________________
    def PARTNER_B_SSP_TOKEN():
        return json.loads(requests.post('https://ssp-partner-b.example/auth',
                                        data={'login': PARTNER_B_SSP_LOGIN_DATA['login'],
                                              'password': PARTNER_B_SSP_LOGIN_DATA['password']}).text)['data']

    def PARTNER_B_DSP_TOKEN():
        return json.loads(requests.post('https://dsp-partner-b.example/token',
                                        data={'login': PARTNER_B_DSP_LOGIN_DATA['login'],
                                              'password': PARTNER_B_DSP_LOGIN_DATA['password']}).text)['data']
    
    def PARTNER_A_SSP_TOKEN():
        return requests.post("https://ssp-partner-a.example/oauth2/token",
                                        data={'grant_type': PARTNER_A_SSP_LOGIN['grant_type'],
                                              'client_id': PARTNER_A_SSP_LOGIN['client_id'],
                                              'username':  PARTNER_A_SSP_LOGIN['username'],
                                              'password':PARTNER_A_SSP_LOGIN['password'],
                                              }
                                        ).json()['access_token']

    def process_xml_data(_xml):
        def extract_data(d):
            date = d.attrib['date']
            impressions = int(float(d.find('impressions').text))
            revenue = float(d.find('revenue').text)
            return date, impressions, revenue
        return map(extract_data, _xml)

    API_PARTNERS_SSP = {
        'ssp-partner-m': {'json': {'urls': [f'https://ssp-partner-m.example/v2/statistics?date_from={start_date_str}&date_to={finish_date_str}',
                                       ],
                              'fun': lambda _json: map(lambda d: (d['date'], int(float(d['base']['shows'])), float(d['base']['spent'])),
                                                       _json['items'][0]['rows']),
                              'headers': {'Authorization': f'Bearer {PARTNER_M_SSP_ACCESS_TOKEN}'},
                              'currency': 'rub',
                              },
                     },

        'ssp-partner-b': {'json': {'urls': [f'https://ssp-partner-b.example/users/{PARTNER_B_SSP_LOGIN_DATA["user_id"]}/report?start_date={start_date_str}&end_date={finish_date_str}',
                                   ],
                          'fun': lambda _json: map(lambda d: (d, int(float(_json['data']['total']['count_imps'][d])),float(_json['data']['total']['net_payable_data'][d])),
                                                   _json['data']['total']['date']),
                          'headers': {'Authorization': f'Token {PARTNER_B_SSP_TOKEN()}'},
                          'currency': 'rub',
                          },
                 },
        'ssp-partner-o': {'json': {'urls': [f'https://ssp-partner-o.example/reporting/dsp?start_date={start_date_str}&end_date={finish_date_str}',
                                    ],
                           'fun': lambda _json: map(lambda d: (d['date'], int(float(d['impressionCount'])), float(d['spent'])),
                                                    _json['data']),
                           },
                  },

        'ssp-partner-s': {'json': {'urls': [f'https://ssp-partner-s.example/api/v1/dsp-report?campaign=display_eur&start={start_date_str}&end={finish_date_str}',
                                     f'https://ssp-partner-s.example/api/v1/dsp-report?campaign=display_us&start={start_date_str}&end={finish_date_str}',
                                     f'https://ssp-partner-s.example/api/v1/dsp-report?campaign=video_eur&start={start_date_str}&end={finish_date_str}',
                                     ],
                            'fun': lambda _json: map(lambda d: (d[0], int(float(d[1]['impressions'])), float(d[1]['revenue'])),
                                                     _json.items()),
                            },
                   },
        'ssp-partner-c': {'xml': {'urls': [f'https://ssp-partner-c.example/dsp-report.xml?start={start_date_str}&end={finish_date_str}',
                                     ],
                            'fun': lambda _xml: map(lambda d: (d.attrib['date'], int(float(d.find('impressions').text)), float(d.find('revenue').text)),
                                                    _xml),
                            },
                    },
        'ssp-partner-d': {'xml': {'urls': [f'https://ssp-partner-d.example/xml-report?format=xml&start={start_date_str}&end={finish_date_str}',
                                        f'https://ssp-partner-d.example/xml-report?format=xml&start={start_date_str}&end={finish_date_str}',
                                        f'https://ssp-partner-d.example/xml-report?format=xml&start={start_date_str}&end={finish_date_str}',
                                         ],
                                'fun': process_xml_data,
                           },
                   },
    }

    # ___________________________________ DSP _______________________________________
    API_PARTNERS_DSP = {
        # dsp-partner-i
        71: {'json': {'urls': [f'https://dsp-partner-i.example/sspReport?start={start_date.strftime("%Y%m%d")}&end={finish_date.strftime("%Y%m%d")}',
                               ],
                      'fun': lambda _json: map(lambda d: (d['date'], int(float(d['imp'])), float(d['revenue'])),
                                               _json['data']),
                      },
             },

        # dsp-partner-o
        65: {'json': {'urls': [f'https://dsp-partner-o.example/v1/reporting?start={start_date_str}&end={finish_date_str}&group=day',
                               ],
                      'fun': lambda _json: map(lambda d: (f"{d['day'][6:11]}-{d['day'][3:5]}-{d['day'][0:2]}",
                                                          int(float(d['impressions'])), float(d['earnings']) / 1000),
                                               _json['data']),
                      'currency': 'rub',
                      },
             },

        # dsp-partner-m
        27: {'json': {'urls': [f'https://dsp-partner-m.example/api/v2/statistics?date_from={start_date_str}&date_to={finish_date_str}',
                               ],
                      'fun': lambda _json: chain.from_iterable(map(lambda d: list(map(lambda r: (r['date'], int(float(r['shows'])), float(r['amount'])),d['rows'])),
                                                                   _json['items'])),
                      'headers': {'Authorization': f'Bearer {PARTNER_M_DSP_ACCESS_TOKEN}'},
                      'currency': 'rub',
                      },
             },

        # dsp-partner-b
        35: {'json': {'urls': [f'https://dsp-partner-b.example/users/{PARTNER_B_DSP_LOGIN_DATA["user_id"]}/sites/chart?start_date={start_date_str}&end_date={finish_date_str}',
                               ],
                      'fun': lambda _json: map(lambda d: (d,int(float(_json['data']['total']['count_imps'][d])),float(_json['data']['total']['total_pub_payable'][d])),
                                               _json['data']['total']['date']),
                      'headers': {'Authorization': f'Token {PARTNER_B_DSP_TOKEN()}'},
                      'currency': 'rub',
                      },
             },

        # dsp-partner-f
        110: {'xml': {'urls': [f'https://dsp-partner-f.example/ssp_xml?start={start_date_str}&end={finish_date_str}',
                               f'https://dsp-partner-f.example/ssp_xml?start={start_date_str}&end={finish_date_str}',
                               ],
                      'fun': lambda _xml: map(lambda d: (d.find('date').text, int(float(d.find('impressions').text)), float(d.find('revenue').text)),
                                              _xml),
                      },
              },
        }

    def agg_list_2keys_2values(dd):  # [ssp/dsp,date,sum(imp),sum(money)]
        q = {}
        for x in dd:
            key = str(x.ssp) + str(x.dsp_id) + str(x.date)
            if key in q:
                q[key].imps += x.imps
                q[key].spent += x.spent
            else:
                q[key] = x
        return list(q.values())

    def process_ssp(ssp):
        dd = []
        for urltype in API_PARTNERS_SSP[ssp]:
            if urltype in ('json', 'xml', 'txt'):
                fun = API_PARTNERS_SSP[ssp][urltype]['fun']
                for url in API_PARTNERS_SSP[ssp][urltype]['urls']:
                    try:
                        print(ssp, urltype, fun, url)
                        head=API_PARTNERS_SSP[ssp][urltype].get('headers', None)
                        print(f'header={head}')
                        res = requests.get(url, headers=head)
                        print(res)
                        print(res.text)
                        if urltype == 'json':
                            _data = json.loads(res.text)
                        elif urltype == 'xml':
                            _data = ET.fromstring(res.text)
                        elif urltype == 'txt':
                            _data = res.text
                        else:
                            _data = None
                        dd = dd + list(
                            map(lambda d: PartnerData(ssp=ssp, date=pd.to_datetime(d[0]), imps=d[1], spent=d[2],currency=API_PARTNERS_SSP[ssp][urltype].get('currency','usd')),
                                list(fun(_data))))
                    except Exception as e:
                        print(e)
        return dd

    def process_dsp(dsp_id):
        dd = []
        for urltype in API_PARTNERS_DSP[dsp_id]:
            if urltype in ('json', 'xml', 'txt'):
                fun = API_PARTNERS_DSP[dsp_id][urltype]['fun']
                for url in API_PARTNERS_DSP[dsp_id][urltype]['urls']:
                    try:
                        print(dsp_id, urltype, fun, url)
                        res = requests.get(url,
                                           headers=API_PARTNERS_DSP[dsp_id][urltype].get('headers', None))
                        print(res)
                        print(res.text)
                        if urltype == 'json':
                            _data = json.loads(res.text)
                        elif urltype == 'xml':
                            _data = ET.fromstring(res.text)
                        elif urltype == 'txt':
                            _data = res.text
                        else:
                            _data = None
                        dd = dd + list(
                            map(lambda d: PartnerData(dsp_id=dsp_id, date=pd.to_datetime(d[0]), imps=d[1], spent=d[2],currency=API_PARTNERS_DSP[dsp_id][urltype].get('currency','usd')),
                                list(fun(_data))))
                    except Exception as e:
                        print(e)
        return dd

    # __________________ SSP _____________________________________________________
    for ssp in API_PARTNERS_SSP:
        data += agg_list_2keys_2values(process_ssp(ssp))

    #__________________________________ DSP _________________________________________________________
    for dsp_id in API_PARTNERS_DSP:
        data += agg_list_2keys_2values(process_dsp(dsp_id))


    ### Processing S - CUSTOM
    print(f"loading partner S")
    transport = CookiesTransport(context=ssl._create_unverified_context())
    result = None
    for ep in [178, 209, 211, 213]:
        try:
            with xmlrpc.client.ServerProxy("https://dsp-partner-s.example/xmlrpc/", transport) as proxy:
                proxy.partner_s.login(PARTNER_S_DSP_LOGIN, PARTNER_S_DSP_TOKEN)

            with xmlrpc.client.ServerProxy("https://dsp-partner-s.example/xmlrpc/", transport) as proxy:
                res = pd.DataFrame(proxy.rtb.get_openrtb_stats((start_date).strftime("%Y-%m-%d %H:%M:%S"), 
                                                                finish_date.strftime("%Y-%m-%d %H:%M:%S"), 
                                                                1, 
                                                                ep))
                res['date'] = res['date_view'].apply(lambda x: pd.to_datetime(str(x), format = '%Y%m%dT%H:%M:%S').date())
                result = pd.concat((result, res))
            print(f"ep={ep} {result}")
        except:
            print(f"bad ep={ep}")
    result = result.groupby('date', as_index = False).agg({'imps':'sum', 'amount':'sum'})
    print("result=")
    print(result)
    data+=[PartnerData(date=pd.to_datetime(row['date']),
                              dsp_id=58,
                              imps=int(row['imps']),
                              spent=float(row['amount']),
                              currency='rub') for _,row in result.iterrows()]


    ### Processing partner G : 123 - CUSTOM
    # G : 123
    tmp_date=start_date
    dsp_id=123
    dd=[]
    fun=lambda _data: map(lambda x: x.split('\t'), filter(lambda d: re.match(r'^[0-9]', d),_data.split('\n')))

    while tmp_date<=finish_date:
        try:
            url=f'https://dsp-partner-g.example/api/v2/reports?start={tmp_date.strftime("%Y-%m-%d")}&end={tmp_date.strftime("%Y-%m-%d")}'
            print(dsp_id,tmp_date, url)
            res = requests.get(url)
            print(res)
            print(res.text)
            _data = res.text
            dd = dd + list(map(lambda d: PartnerData(dsp_id=dsp_id, date=pd.to_datetime(tmp_date), imps=int(float(d[0])), spent=float(d[1]),currency='rub'),
                               list(fun(_data))))
            tmp_date+=datetime.timedelta(days=1)
        except Exception as e:
            print(e)
    data += agg_list_2keys_2values(dd)


    ### Processing partner B dsp - CUSTOM
    print("Processing partner B dsp")
    curr_date = start_date
    b_dsp_id = 376
    b_dsp_data = []

    fun = lambda _json: _json['statistic']
    while curr_date <= finish_date:
        try:
            url = 'https://dsp-partner-b.example/stats'
            payload = {
                "start_date": curr_date.strftime("%d-%m-%Y"),
                "end_date": curr_date.strftime("%d-%m-%Y"),
                "field_names": ["impressions", "clicks", "revenue"],
                "group_by": ["site_id", "placement_id"]
            }
            res = requests.post(url, json=payload, headers={'Authorization': f'Bearer {DSP_B_ACCESS_TOKEN}'})
            _data = res.json()
            total_impressions = 0
            total_revenue = 0.0
            
            # Aggregating by data from list in one curr_date
            for item in fun(_data):
                total_impressions += int(item['impressions'])
                total_revenue += float(item['revenue'])

            # Create one object PartnerData for curr_date
            b_dsp_data.append(PartnerData(
                dsp_id=b_dsp_id,
                date=pd.to_datetime(curr_date),
                imps=total_impressions,
                spent=total_revenue,
                # currency='rub'
            ))

            curr_date += datetime.timedelta(days=1)
        except Exception as e:
            print("b dsp error", e)

    data += agg_list_2keys_2values(b_dsp_data)

    print("FINAL PartnerData for partners :", data)

    query = "INSERT INTO dbname.partner_data (*) VALUES"
    client = Client(CLICKHOUSE_CONFIG_PROD['host'])
    client.execute(query, (asdict(row) for row in data))


def Partners_data_loader(start_date=None, finish_date=None):
    job = queue("load_insert_data", service="api").enqueue(load_insert_data, start_date, finish_date)
    return { 'job_id': job.get_id() }, 200
