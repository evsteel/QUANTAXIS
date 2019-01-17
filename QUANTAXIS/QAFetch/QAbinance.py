"""
币安api
具体api文档参考:https://github.com/binance-exchange/binance-official-api-docs/blob/master/rest-api.md
"""
import requests
import json
import datetime
import time
import ssl
import pandas as pd
from requests.exceptions import ConnectTimeout

from urllib.parse import urljoin
from QUANTAXIS.QAUtil.QAcrypto import TIMEOUT, ILOVECHINA
from QUANTAXIS.QAUtil import QA_util_log_info, QA_util_log_debug, QA_util_log_expection

proxies = {
  'http': 'http://127.0.0.1:1080',
  'https': 'http://127.0.0.1:1080',
}

Binance_base_url = "https://api.binance.com"

columne_names = ['start_time', 'open', 'high', 'low', 'close', 'volume', 'close_time',
                 'quote_asset_volume', 'num_trades', 'buy_base_asset_volume',
                 'buy_quote_asset_volume', 'Ignore']

def QA_fetch_binance_symbols():
    url = urljoin(Binance_base_url, "/api/v1/exchangeInfo")
    try:
        req = requests.get(url, timeout=TIMEOUT, proxies=proxies)
    except ConnectTimeout:
        raise ConnectTimeout(ILOVECHINA)
    body = json.loads(req.content)
    return body["symbols"]


def QA_fetch_binance_kline(symbol, start_time, end_time, frequency):
    datas = list()
    start_time *= 1000
    end_time *= 1000
    while start_time < end_time:
        url = urljoin(Binance_base_url, "/api/v1/klines")
        try:
            req = requests.get(url, params={"symbol": symbol, "interval": frequency,
                                            "startTime": int(start_time),
                                            "endTime": int(end_time)}, timeout=TIMEOUT, proxies=proxies)
            # 防止频率过快被断连
            time.sleep(1)
        except ConnectTimeout:
            raise ConnectTimeout(ILOVECHINA)
        except (ssl.SSLError, requests.exceptions.SSLError) as ex:
            QA_util_log_info(ex)
            time.sleep(120)
            req = requests.get(url, timeout=TIMEOUT, proxies=proxies)
        except Exception as ex:
            break
        klines = json.loads(req.content)
        if len(klines) == 0:
            break
        datas.extend(klines)
        start_time = klines[-1][6]
    if len(datas) == 0:
        return None
    frame = pd.DataFrame(datas)
    frame.columns = columne_names
    frame['symbol'] = symbol
    return json.loads(frame.to_json(orient='records'))


if __name__ == '__main__':
    print(QA_fetch_binance_symbols())