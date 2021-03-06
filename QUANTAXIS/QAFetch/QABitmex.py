import requests
import json
import ssl
from datetime import datetime
from dateutil.relativedelta import relativedelta
import time
from requests.exceptions import ConnectTimeout
from QUANTAXIS.QAUtil.QASetting import proxies, UTC_TIME_FORMAT
from QUANTAXIS.QAUtil import QA_util_log_info, QA_util_log_debug, QA_util_log_expection

# 具体API文档可见
# https://www.bitmex.com/api/explorer/

Bitmex_base_url = "https://www.bitmex.com/api/v1"


BINSIZE_DICT = {
    "1m": relativedelta(minutes=1),
    "5m": relativedelta(minutes=5),
    "1h": relativedelta(hours=1),
    "1d": relativedelta(days=1),
}

TIMEOUT = 120


# 获取所有交易对
def QA_fetch_bitmex_symbols(active=False):
    if active:
        url = '%s/instrument/active' % Bitmex_base_url
    else:
        url = '%s/instrument' % Bitmex_base_url
    try:
        req = requests.get(url, timeout=TIMEOUT, proxies=proxies)
    except ConnectTimeout:
        raise ConnectTimeout('Bitmex connect timeout when getting symbols.')
    body = json.loads(req.content)
    return body


# 获取交易费率
def QA_fetch_bitmex_funding(symbol='XBTUSD',
                            count=100,
                            reverse='false',
                            startTime='',
                            endTime='',
                            proxies=proxies):

    url = '%s/funding?symbol=%s&count=%d&reverse=%s&tartTime=%s&endTime=%s' \
          % (Bitmex_base_url, symbol, count, reverse, startTime, endTime)
    try:
        req = requests.get(url, timeout=TIMEOUT, proxies=proxies)
    except ConnectTimeout:
        raise ConnectTimeout('Bitmex connect timeout when getting symbols.')
    except (ssl.SSLError, requests.exceptions.SSLError) as ex:
        QA_util_log_info(ex)
    body = json.loads(req.content)
    return body


# 获取聚合后的交易记录（k线数据）
def QA_fetch_bitmex_kline(symbol="XBTUSD",
                          count=100,
                          startTime="",
                          endTime='',
                          binSize="1m",
                          partial='false',
                          reverse='false',
                          proxies=proxies):
    body = ''

    url = '%s/trade/bucketed?' \
          'binSize=%s&partial=%s&symbol=%s&count=%d&reverse=%s&startTime=%s&endTime=%s' \
          % (Bitmex_base_url, binSize, partial, symbol, count, reverse, startTime, endTime)
    try:
        req = requests.get(url, timeout=TIMEOUT, proxies=proxies)
        remaining = int(req.headers['x-ratelimit-remaining'])
        if remaining <20:
            time.sleep(5)
        elif remaining <10:
            time.sleep(10)
        elif remaining <3:
            time.sleep(30)
    except ConnectTimeout:
        raise ConnectTimeout('Bitmex connect timeout when getting kline.')
    except (ssl.SSLError, requests.exceptions.SSLError) as ex:
        QA_util_log_info(ex)
    except Exception as ex:
        QA_util_log_expection(ex)

    return json.loads(req.content)


if __name__ == '__main__':
    pass