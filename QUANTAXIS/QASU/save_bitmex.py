import datetime
from datetime import datetime
import time
from QUANTAXIS.QAUtil import DATABASE, QASETTING, QA_util_log_info, QA_util_log_expection

from QUANTAXIS.QAFetch.QABitmex import QA_fetch_bitmex_symbols, QA_fetch_bitmex_funding, QA_fetch_bitmex_kline
from QUANTAXIS.QAUtil.QAcrypto import QA_SU_save_symbols
import pymongo


def QA_SU_save_bitmex(client=DATABASE, frequency='1m', symbol=''):

    symbol_list = list()
    if type(symbol) == str and len(symbol) > 0:
        symbol_list.append(symbol)
    elif type(symbol) == list:
        symbol_list = symbol
    else:
        symbol_list = QA_fetch_bitmex_symbols()['symbols']

    for index, symbol in enumerate(symbol_list):

        QA_util_log_info('The {} of Total {}'.format
                         (symbol, len(symbol_list)))

        col_name = 'bitmex_%s_%s' % (frequency, symbol)
        col = client[col_name]
        col.create_index(
            [("symbol", pymongo.ASCENDING), ("timestamp", pymongo.ASCENDING)], unique=True)

        QA_util_log_info('DOWNLOAD PROGRESS {} '.format(str(
            float(index / len(symbol_list) * 100))[0:4] + '%')
                         )
        ref = col.find({"symbol": symbol}).sort("timestamp", -1)

        if ref.count() > 0:
            start_timestamp_str = ref.next()['timestamp']
            QA_util_log_info('UPDATE_SYMBOL \n Trying updating {} from {}'.format(
                symbol, start_timestamp_str))
        else:
            start_timestamp_str = ''
            QA_util_log_info('NEW_SYMBOL \n Trying downloading {} from {}'.format(
                symbol, start_timestamp_str))

        data = QA_fetch_bitmex_kline(symbol=symbol, startTime=start_timestamp_str, binSize=frequency)
        if data is None or len(data) == 0:
            QA_util_log_info('SYMBOL {} from {} has no new data'.format(symbol, start_timestamp_str))
            continue
        try:
            col.insert(data)
        except Exception as ex:
            QA_util_log_expection(ex)
            QA_util_log_expection(data)


def QA_SU_save_bitmex_1min(client=DATABASE, symbol='XBTUSD', frequency='1m'):
    QA_SU_save_bitmex(client=DATABASE, frequency='1m', symbol=symbol)


def QA_SU_save_bitmex_1day(client=DATABASE, symbol='XBTUSD', frequency='1m'):
    QA_SU_save_bitmex(client=DATABASE, frequency="1d", symbol=symbol)


def QA_SU_save_bitmex_1hour(client=DATABASE, symbol='XBTUSD', frequency='1m'):
    QA_SU_save_bitmex(client=DATABASE, frequency="1h", symbol=symbol)


def QA_SU_save_bitmex_symbol(client=DATABASE):
    client.drop_collection("bitmex_symbols")
    QA_util_log_info("Delete all bitmex symbols collections")

    symbol_list = QA_fetch_bitmex_symbols()
    QA_util_log_info("Downloading the new symbols")

    col = client['bitmex_symbols']
    col.insert(symbol_list)
    QA_util_log_info("Update bitmex Symbols finished")

# FREQUANCY_DICT = {
#     "1m": relativedelta(minutes=-1),
#     "1d": relativedelta(days=-1),
#     "1h": relativedelta(hours=-1)
# }


# def QA_SU_save_bitmex(frequency):
#     symbol_list = QA_fetch_bitmex_symbols(active=True)
#     symbol_list = symbol_list
#     col = QASETTING.client.bitmex[frequency]
#     col.create_index(
#         [("symbol", pymongo.ASCENDING), ("timestamp", pymongo.ASCENDING)], unique=True)

#     end = datetime.datetime.now(tzutc()) + relativedelta(days=-1, hour=0, minute=0, second=0, microsecond=0)

#     for index, symbol_info in enumerate(symbol_list):
#         QA_util_log_info('The {} of Total {}'.format
#                          (symbol_info['symbol'], len(symbol_list)))
#         QA_util_log_info('DOWNLOAD PROGRESS {} '.format(str(
#             float(index / len(symbol_list) * 100))[0:4] + '%')
#                          )
#         ref = col.find({"symbol": symbol_info['symbol']}).sort("timestamp", -1)

#         if ref.count() > 0:
#             start_stamp = ref.next()['timestamp'] / 1000
#             start_time = datetime.datetime.fromtimestamp(start_stamp+1,tz=tzutc())
#             QA_util_log_info('UPDATE_SYMBOL {} Trying updating {} from {} to {}'.format(
#                 frequency, symbol_info['symbol'], start_time, end))
#         else:
#             start_time = symbol_info.get('listing', "2018-01-01T00:00:00Z")
#             start_time = parse(start_time)
#             QA_util_log_info('NEW_SYMBOL {} Trying downloading {} from {} to {}'.format(
#                 frequency, symbol_info['symbol'], start_time, end))

#         data = QA_fetch_bitmex_kline(symbol_info['symbol'],
#                                       start_time, end, frequency)
#         if data is None:
#             QA_util_log_info('SYMBOL {} from {} to {} has no data'.format(
#                 symbol_info['symbol'], start_time, end))
#             continue
#         col.insert_many(data)


if __name__ == '__main__':
    QA_SU_save_bitmex(symbol='XBTUSD', frequency='1d')
