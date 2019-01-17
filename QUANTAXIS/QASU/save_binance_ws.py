import json
from websocket import create_connection, WebSocketApp

from QUANTAXIS.QAUtil.QASetting import proxy_url, proxy_port
from QUANTAXIS.QAFetch.QAbinance import QA_fetch_binance_symbols
from QUANTAXIS.QAUtil import (QASETTING, QA_util_log_info, QA_util_log_debug, QA_util_log_expection)

WSS_URL = 'wss://stream.binance.com:9443'
col_trade = QASETTING.client.binance['trade']
col_depth = QASETTING.client.binance['depth']


def QA_SU_subscribe_binance_websocket(symbol='', is_depth=False):
    streamlist = []
    if type(symbol) == str:
        if symbol.find(',') > 0:
            symbol = symbol.split(',')

    if type(symbol) == str and len(symbol) > 0:
        streamlist.append('%s@trade' % symbol)
        if is_depth:
            streamlist.append('%s@depth' % symbol)
    elif type(symbol) == list:
        for symbol_name in symbol:
            streamlist.append('%s@trade' % symbol_name)
            if is_depth:
                streamlist.append('%s@depth' % symbol_name)
    else:
        symbol_infolist = QA_fetch_binance_symbols()
        for index, symbol_info in enumerate(symbol_infolist):
            streamlist.append('%s@trade' % symbol_info['symbol'])
            if is_depth:
                streamlist.append('%s@depth' % symbol_info['symbol'])

    if len(streamlist) > 1:
        stream_string = '/stream?streams='
        for stream in streamlist:
            stream_string = '%s%s/' % (stream_string, stream)
    else:
        stream_string = '/ws/%s' % streamlist[0]
    stream_string = stream_string.lower()
    QA_util_log_debug('Binance subscribe string: %s' % stream_string)
    ws = WebSocketApp('%s%s' % (WSS_URL, stream_string), on_message=on_message, on_error=on_error, on_close=on_close)
    # ws = WebSocketApp('%s' % WSS_URL, on_message=on_message, on_error=on_error, on_close=on_close)
    ws.on_open = on_open
    while True:
        ws.run_forever(http_proxy_host=proxy_url, http_proxy_port=proxy_port)


def on_message(ws, message):
    QA_util_log_info(message)
    body = json.loads(message)
    if body['stream'][-5:] == 'depth':
        col_depth.insert_one(body)
    else:
        col_trade.insert_one(body)


def on_error(ws, error):
    QA_util_log_expection(error)


def on_close(ws):
    QA_util_log_info('binance trade subscribe websocket closed')
    ws.close()


def on_open(ws):
    QA_util_log_info('binance trade subscribe websocket open')


if __name__ == '__main__':
    QA_SU_subscribe_binance_websocket('', is_depth=True)
