import requests
import json
from datetime import datetime
from dateutil.relativedelta import relativedelta
import time
from requests.exceptions import ConnectTimeout
from QUANTAXIS.QAUtil.QASetting import proxies, UTC_TIME_FORMAT
from QUANTAXIS.QAUtil import DATABASE, QA_util_log_info, QA_util_log_debug, QA_util_log_expection

# 具体API文档可见
# https://www.bitmex.com/api/explorer/

Bitmex_base_url = "https://www.bitmex.com"


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
        QA_util_log_info('Invalid bitmex %s_%s startTime' % (binSize, symbol))
        try:
            req = requests.get(url, timeout=TIMEOUT, proxies=proxies)
            # time.sleep(0.5)
        except ConnectTimeout:
            raise ConnectTimeout('Bitmex connect timeout when getting symbols.')
        startTime_str = json.loads(req.content)[0]['timestamp']
        startTime_datetime = datetime.strptime(startTime_str, UTC_TIME_FORMAT)
        QA_util_log_info('Bitmex %s_%s startTime %s is' % (binSize, symbol,startTime_str))

    try:
        finalEndTime_datetime = datetime.strptime(endTime, UTC_TIME_FORMAT)
    except:
        # Get binSize/Symbol's last record timestamp
        url = '%s/api/v1/trade/bucketed?binSize=%s&partial=%s&symbol=%s&count=%d&reverse=%s&startTime=%s&endTime=%s' \
              % (Bitmex_base_url, binSize, partial, symbol, count, 'true', '', '')
        QA_util_log_info('Invalid bitmex %s_%s finalTime' % (binSize, symbol))
        try:
            req = requests.get(url, timeout=TIMEOUT, proxies=proxies)
            time.sleep(0.5)
        except ConnectTimeout:
            raise ConnectTimeout('Bitmex connect timeout when getting symbols.')
        finalEndTime_str = json.loads(req.content)[0]['timestamp']
        finalEndTime_datetime = datetime.strptime(finalEndTime_str, UTC_TIME_FORMAT)
        QA_util_log_info('Bitmex %s_%s finalTime %s is' % (binSize, symbol, finalEndTime_str))
    body = []

    # begin fetching loop
    QA_util_log_info("Fetching bitmex %s_%s from %s to %s"
                     % (binSize, symbol, datetime.strftime(startTime_datetime, UTC_TIME_FORMAT),
                        datetime.strftime(finalEndTime_datetime, UTC_TIME_FORMAT)))
    while startTime_datetime != finalEndTime_datetime:
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

        QA_util_log_info('Fetch completed, bitmex %s_%s from %s to %s, total %d records'
                         % (binSize, symbol, startTime_str, finalEndTime_str, len(body)))
    return body

# MAX_HISTORY = 750


# def QA_fetch_bitmex_symbols(active=False):
#     if active:
#         url = urljoin(Bitmex_base_url, "instrument/active")
#     else:
#         url = urljoin(Bitmex_base_url, "instrument")
#     try:
#         req = requests.get(url, params={"count":500}, timeout=TIMEOUT)
#     except ConnectTimeout:
#         raise ConnectTimeout(ILOVECHINA)
#     body = json.loads(req.content)
#     return body


# def QA_fetch_bitmex_kline(symbol, start_time, end_time, frequency):
#     datas = list()
#     while start_time < end_time:
#         url = urljoin(Bitmex_base_url, "trade/bucketed")
#         try:
#             req = requests.get(url, params={"symbol": symbol, "binSize": frequency,
#                                             "startTime": start_time.isoformat(),
#                                             "endTime": end_time.isoformat(),
#                                             "count":MAX_HISTORY}, timeout=TIMEOUT)
#         except ConnectTimeout:
#             raise ConnectTimeout(ILOVECHINA)
#         # 防止频率过快被断连
#         remaining = int(req.headers['x-ratelimit-remaining'])
#         if remaining <20:
#             time.sleep(0.5)
#         elif remaining <10:
#             time.sleep(5)
#         elif remaining <3:
#             time.sleep(30)

#         klines = json.loads(req.content)
#         if len(klines) == 0:
#             break
#         datas.extend(klines)
#         start_time = parse(klines[-1].get("timestamp")) + relativedelta(second=+1)
#     if len(datas) == 0:
#         return None
#     frame = pd.DataFrame(datas)
#     frame['timestamp'] = pd.to_datetime(frame['timestamp'])
#     return json.loads(frame.to_json(orient='records'))

if __name__ == '__main__':
    print(QA_fetch_bitmex_funding(symbol='XBTUSD'))
    # print(QA_fetch_bitmex_kline(symbol='XBTUSD', binSize='1d'))
