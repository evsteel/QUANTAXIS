import requests
import json
from datetime import datetime
from dateutil.relativedelta import relativedelta
import time
from requests.exceptions import ConnectTimeout
from QUANTAXIS.QAUtil.QASetting import proxies

# 具体API文档可见
# https://www.bitmex.com/api/explorer/

Bitmex_base_url = "https://www.bitmex.com"
UTC_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"


BINSIZE_DICT = {
    "1m": relativedelta(minutes=1),
    "5m": relativedelta(minutes=5),
    "1h": relativedelta(hours=1),
    "1d": relativedelta(days=1),
}

TIMEOUT = 120

# 获取所有交易对
def QA_fetch_bitmex_symbols(proxies=proxies):
    url = '%s/api/v1/instrument/activeIntervals' % Bitmex_base_url
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

    url = '%s/api/v1/funding?symbol=%s&count=%d&reverse=%s&tartTime=%s&endTime=%s' \
          % (Bitmex_base_url, symbol, count, reverse, startTime, endTime)
    try:
        req = requests.get(url, timeout=TIMEOUT, proxies=proxies)
    except ConnectTimeout:
        raise ConnectTimeout('Bitmex connect timeout when getting symbols.')
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

    try:
        startTime_datetime = datetime.strptime(startTime, UTC_TIME_FORMAT)
    except:
        # Get binSize/Symbol's first record timestamp
        url = '%s/api/v1/trade/bucketed?' \
              'binSize=%s&partial=%s&symbol=%s&count=%d&reverse=%s&startTime=%s&endTime=%s' \
              % (Bitmex_base_url, binSize, partial, symbol, count, 'false', '', '')
        try:
            req = requests.get(url, timeout=TIMEOUT, proxies=proxies)
            time.sleep(0.5)
        except ConnectTimeout:
            raise ConnectTimeout('Bitmex connect timeout when getting symbols.')
        startTime_str = json.loads(req.content)[0]['timestamp']
        startTime_datetime = datetime.strptime(startTime_str, UTC_TIME_FORMAT)

    try:
        finalEndTime_datetime = datetime.strptime(endTime, UTC_TIME_FORMAT)
    except:
        # Get binSize/Symbol's last record timestamp
        url = '%s/api/v1/trade/bucketed?binSize=%s&partial=%s&symbol=%s&count=%d&reverse=%s&startTime=%s&endTime=%s' \
              % (Bitmex_base_url, binSize, partial, symbol, count, 'true', '', '')
        try:
            req = requests.get(url, timeout=TIMEOUT, proxies=proxies)
            time.sleep(0.5)
        except ConnectTimeout:
            raise ConnectTimeout('Bitmex connect timeout when getting symbols.')
        finalEndTime_str = json.loads(req.content)[0]['timestamp']
        finalEndTime_datetime = datetime.strptime(finalEndTime_str, UTC_TIME_FORMAT)

    body = []

    # begin fetching loop
    while True:
        # Calculate current request's endTime, if new endTime > finalEndTime then endTime = finalEndtime
        endTime_datetime = (startTime_datetime + BINSIZE_DICT[binSize] * count) \
            if (startTime_datetime + BINSIZE_DICT[binSize] * count <= finalEndTime_datetime) \
            else finalEndTime_datetime
        startTime_str = datetime.strftime(startTime_datetime, UTC_TIME_FORMAT)
        endTime_str = datetime.strftime(endTime_datetime, UTC_TIME_FORMAT)

        url = '%s/api/v1/trade/bucketed?' \
              'binSize=%s&partial=%s&symbol=%s&count=%d&reverse=%s&startTime=%s&endTime=%s' \
              % (Bitmex_base_url, binSize, partial, symbol, count, reverse, startTime_str, endTime_str)
        try:
            req = requests.get(url, timeout=TIMEOUT, proxies=proxies)
            time.sleep(0.5)
        except ConnectTimeout:
            raise ConnectTimeout('Bitmex connect timeout when getting symbols.')
        body += json.loads(req.content)

        # finish query, break loop
        if endTime_datetime == finalEndTime_datetime:
            break
        else:
            # Calculate next request's startTime
            startTime_datetime = endTime_datetime + BINSIZE_DICT[binSize]
    return body

if __name__ == '__main__':
    print(QA_fetch_bitmex_funding(symbol='XBTUSD'))
    # print(QA_fetch_bitmex_kline(symbol='XBTUSD', binSize='1d'))