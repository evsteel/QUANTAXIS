import datetime
from datetime import datetime
import time
from QUANTAXIS.QAUtil import QASETTING, QA_util_log_info, QA_util_log_expection
from QUANTAXIS.QAFetch.QABitmex import QA_fetch_bitmex_symbols, QA_fetch_bitmex_funding, QA_fetch_bitmex_kline
from QUANTAXIS.QAUtil.QAcrypto import QA_SU_save_symbols
import pymongo


def QA_SU_save_bitmex(client=QASETTING.client, frequency='1m', symbol=''):

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

    for index, symbol in enumerate(symbol_list):

        QA_util_log_info('The {} of Total {}'.format
                         (symbol, len(symbol_list)))
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
            QA_util_log_info('NEW_SYMBOL {}'.format(symbol))

        data = QA_fetch_bitmex_kline(symbol=symbol, startTime=start_timestamp_str, binSize=frequency)
        if data is None or len(data) == 0:
            QA_util_log_info('SYMBOL {} from {} has no new data'.format(symbol, start_timestamp_str))
            continue
        try:
            col.insert(data)
        except Exception as ex:
            QA_util_log_expection(ex)
            QA_util_log_expection(data)


def QA_SU_save_bitmex_1min(client=QASETTING.client, symbol='XBTUSD'):
    QA_SU_save_bitmex(client=client, frequency='1m', symbol=symbol)


def QA_SU_save_bitmex_1day(client=QASETTING.client, symbol='XBTUSD'):
    QA_SU_save_bitmex(client=client, frequency="1d", symbol=symbol)


def QA_SU_save_bitmex_1hour(client=QASETTING.client, symbol='XBTUSD'):
    QA_SU_save_bitmex(client=client, frequency="1h", symbol=symbol)


def QA_SU_save_bitmex_symbol(client=QASETTING.client):
    QA_SU_save_symbols(QA_fetch_bitmex_symbols, "bitmex")

# def QA_SU_save_bitmex_symbol(client=DATABASE):
#     client.drop_collection("bitmex_symbols")
#     QA_util_log_info("Delete all bitmex symbols collections")
#
#     symbol_list = QA_fetch_bitmex_symbols()
#     QA_util_log_info("Downloading the new symbols")
#
#     col = client['bitmex_symbols']
#     col.insert(symbol_list)
#     QA_util_log_info("Update bitmex Symbols finished")


if __name__ == '__main__':
    QA_SU_save_bitmex(symbol='XBTUSD', frequency='1d')

    # QA_SU_save_bitmex_symbol()