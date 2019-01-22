import datetime
from datetime import datetime
import time
from QUANTAXIS.QAUtil import QASETTING, QA_util_log_info, QA_util_log_expection
from QUANTAXIS.QAFetch.QABitmex import QA_fetch_bitmex_symbols, QA_fetch_bitmex_funding, QA_fetch_bitmex_kline
from QUANTAXIS.QAUtil.QAcrypto import QA_SU_save_symbols
from dateutil.relativedelta import relativedelta
from QUANTAXIS.QAUtil.QASetting import UTC_TIME_FORMAT
import pymongo


BINSIZE_DICT = {
    "1m": relativedelta(minutes=1),
    "5m": relativedelta(minutes=5),
    "1h": relativedelta(hours=1),
    "1d": relativedelta(days=1),
}


def QA_SU_save_bitmex(client=QASETTING.client, frequency='1m', symbol=''):

    count = 100

    col = client.bitmex[frequency]
    col.create_index([("symbol", pymongo.ASCENDING), ("timestamp", pymongo.ASCENDING)], unique=True)

    symbol_list = list()
    if type(symbol) == str and len(symbol) > 0:
        symbol_list.append(symbol)
    elif type(symbol) == list:
        symbol_list = symbol
    else:
        for index, symbol_info in enumerate(QA_fetch_bitmex_symbols(active=True)):
            symbol_list.append(symbol_info['symbol'])

    # begin all symbol update loop
    for index, symbol in enumerate(symbol_list):

        QA_util_log_info('The {} of Total {}'.format(symbol, len(symbol_list)))
        QA_util_log_info('DOWNLOAD PROGRESS {} '.format(str(float(index / len(symbol_list) * 100))[0:4] + '%'))

        ref = col.find({"symbol": symbol}).sort("timestamp", -1)

        # get symbol start time
        if ref.count() > 0:
            start_timestamp_str = ref.next()['timestamp']
            QA_util_log_info('UPDATE_SYMBOL Trying updating {}_{} from {}'.format(frequency, symbol, start_timestamp_str))
        else:
            start_timestamp_str = QA_fetch_bitmex_kline(symbol=symbol, count=1, reverse='false', binSize=frequency)[0]['timestamp']
            QA_util_log_info('No %s_%s record found, fetching record start time from server %s' %
                             (frequency, symbol, start_timestamp_str))

        startTime_datetime = datetime.strptime(start_timestamp_str, UTC_TIME_FORMAT) + BINSIZE_DICT[frequency]

        # get symbol end time
        end_timestamp_str = QA_fetch_bitmex_kline(symbol=symbol, count=1, reverse='true', binSize=frequency)[0]['timestamp']
        finalEndTime_datetime = datetime.strptime(end_timestamp_str, UTC_TIME_FORMAT)

        while startTime_datetime < finalEndTime_datetime:
            # Calculate current request's endTime, if new endTime > finalEndTime then endTime = finalEndtime
            endTime_datetime = (startTime_datetime + BINSIZE_DICT[frequency] * count) \
                if (startTime_datetime + BINSIZE_DICT[frequency] * count <= finalEndTime_datetime) \
                else finalEndTime_datetime
            startTime_str = datetime.strftime(startTime_datetime, UTC_TIME_FORMAT)
            endTime_str = datetime.strftime(endTime_datetime, UTC_TIME_FORMAT)

            data = QA_fetch_bitmex_kline(symbol=symbol, startTime=startTime_str, binSize=frequency, endTime=endTime_str)
            if type(data) == list and len(data) > 1:
                col.insert_many(data)
            else:
                col.insert(data)

            # finish query, break loop
            if endTime_datetime == finalEndTime_datetime:
                break
            else:
                startTime_datetime = endTime_datetime + BINSIZE_DICT[frequency]


def QA_SU_save_bitmex_1min(client=QASETTING.client, symbol='XBTUSD'):
    QA_SU_save_bitmex(client=client, frequency='1m', symbol=symbol)


def QA_SU_save_bitmex_1day(client=QASETTING.client, symbol='XBTUSD'):
    QA_SU_save_bitmex(client=client, frequency="1d", symbol=symbol)


def QA_SU_save_bitmex_1hour(client=QASETTING.client, symbol='XBTUSD'):
    QA_SU_save_bitmex(client=client, frequency="1h", symbol=symbol)


def QA_SU_save_bitmex_symbol():
    QA_SU_save_symbols(QA_fetch_bitmex_symbols, "bitmex")


if __name__ == '__main__':
    QA_SU_save_bitmex(symbol='XBTUSD', frequency='1m')
    # QA_SU_save_bitmex_symbol()
