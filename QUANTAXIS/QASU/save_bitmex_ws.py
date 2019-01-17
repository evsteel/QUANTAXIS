import json
from websocket import create_connection, WebSocketApp

from QUANTAXIS.QAUtil.QASetting import proxy_url, proxy_port
from QUANTAXIS.QAFetch.QABitmex import QA_fetch_bitmex_symbols
from QUANTAXIS.QAUtil import (QASETTING, QA_util_log_info, QA_util_log_debug, QA_util_log_expection)

WSS_URL='wss://www.bitmex.com/realtime?'
col_trade = QASETTING.client.bitmex['trade']
col_orderBookL2 = QASETTING.client.bitmex['orderBookL2']


def QA_SU_subscribe_bitmex_websocket(symbol='', is_orderbookl2=False):

    symbol_list = []
    if type(symbol) == str:
        if symbol.find(',') > 0:
            symbol = symbol.split(',')

    if type(symbol) == str and len(symbol) > 0:
        symbol_list = [symbol]
        if is_orderbookl2:
            symbol_list.append('OrderBookL2:' % symbol)
    elif type(symbol) == list:
        symbol_list = symbol
    else:
        symbol_infolist = QA_fetch_bitmex_symbols(active=True)
        for index, symbol_info in enumerate(symbol_infolist):
            symbol_list.append('%s' % symbol_info['symbol'])

    subscribe_trade_str = '%s%s' % ('trade:', ',trade:'.join((str(symbol) for symbol in symbol_list)))
    if is_orderbookl2:
        subscribe_orderBookL2_str = '%s%s' % ('orderBookL2:', ',orderBookL2:'.join((str(symbol) for symbol in symbol_list)))
        subscribe_str = '%s,%s' % (subscribe_trade_str, subscribe_orderBookL2_str)
    else:
        subscribe_str = subscribe_trade_str
    stream_string = 'subscribe=%s' % subscribe_str

    ws = WebSocketApp('%s%s' % (WSS_URL, stream_string), on_message=on_message, on_error=on_error, on_close=on_close)
    ws.on_open = on_open
    while True:
        ws.run_forever(http_proxy_host=proxy_url, http_proxy_port=proxy_port)


def on_message(ws, message):
    QA_util_log_info(message)
    print(message)
    body = json.loads(message)
    if body['table'] == 'orderBookL2':
        if len(col_orderBookL2) == 1:
            col_orderBookL2.insert_one(body)
        else:
            col_orderBookL2.insert_many(body)
    elif body['table'] == 'trade':
        if len(col_trade) == 1:
            col_trade.insert_one(body)
        else:
            col_trade.insert_many(body)


def on_close(ws):
    QA_util_log_info('bitmex trade subscribe websocket closed')
    ws.close()


def on_open(ws):
    QA_util_log_info('bitmex trade subscribe websocket open')


def on_error(ws, error):
    QA_util_log_expection(error)


if __name__ == '__main__':
    QA_SU_subscribe_bitmex_trade()
