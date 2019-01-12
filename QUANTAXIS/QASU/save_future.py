import QUANTAXIS as QA
from QUANTAXIS.QAUtil import (QASETTING, QA_util_log_info)
from QUANTAXIS.QAFetch import QATdx
import pymongo, time, json
import socket
from QUANTAXIS.QAUtil.QASetting import DATABASE

DEFAULT_START_DATE = '1990-01-01'
DEFAULT_START_TIME = '00:00:00'

future_product_list = QATdx.QA_fetch_get_future_list().code


def QA_SU_save_future_day(client=DATABASE):
    err_timeout_product_list = []
    err_unknown_product_list = []
    no_data_list = []
    succ_product_list = []
    QA_util_log_info('Updating future day, total %d products' % len(future_product_list))

    for index, product_code in enumerate(future_product_list):
        col = QASETTING.client.future_day[product_code]
        col.create_index([('date', pymongo.DESCENDING)], unique=True)
        ref = col.find({"code": product_code}).sort('date', pymongo.DESCENDING)
        start_date = DEFAULT_START_DATE
        if ref.count() > 0:
            start_date = ref.next()['date']
            col.delete_one({'date': start_date})
        end_date = time.strftime('%Y-%m-%d')
        if start_date != end_date:
            try:
                print('UPDATE %s from %s to %s' % (product_code, start_date, end_date), end="")
                future_df = QATdx.QA_fetch_get_future_day(product_code,
                                                          start_date=start_date,
                                                          end_date=end_date)
                if len(future_df) > 0:
                    future_json = future_df.to_json(orient='records')
                    col.insert_many(json.loads(future_json))
                    print(' OK!')
                else:
                    no_data_list.append(product_code)
                    print(' NO DATA FROM TDX!')
                succ_product_list.append(product_code)
            except socket.timeout as ex:
                err_timeout_product_list.append('%s: error.socket.timeout' % product_code)
                print(' TIMEOUT!')
            except Exception as ex:
                err_unknown_product_list.append('%s: %s\n %s' % (product_code, ex, future_df))
                print(' UNKNOWN_ERROR!')
        else:
            print('%s is up to date %s' % (product_code, end_date))
    QA_util_log_info('Update future summary\n Total succeed: %d\n Total error:%d' %
                     (len(succ_product_list),
                      len(err_timeout_product_list) + len(err_unknown_product_list)))
    if len(err_timeout_product_list) > 0:
        for each_err_log in err_timeout_product_list:
            QA_util_log_info(each_err_log)
    if len(err_unknown_product_list) > 0:
        for each_err_log in err_unknown_product_list:
            QA_util_log_info(each_err_log)
    if len(no_data_list) > 0:
        QA_util_log_info('No data from TDX: %s' % no_data_list)


def QA_SU_save_future_min():
    err_timeout_product_list = []
    err_unknown_product_list = []
    succ_product_list = []
    no_data_list = []
    QA_util_log_info('Updating future min, total %d products' % len(future_product_list))

    for index, product_code in enumerate(future_product_list):
        col = QASETTING.client.future_1min[product_code]
        col.create_index([('datetime', pymongo.DESCENDING)], unique=True)
        ref = col.find({"code": product_code}).sort('datetime', pymongo.DESCENDING)
        start = '%s %s' % (DEFAULT_START_DATE, DEFAULT_START_TIME)
        if ref.count() > 0:
            start = ref.next()['datetime']
            col.delete_one({'datetime': start})
        end = time.strftime('%Y-%m-%d %H:%M:%S')
        if start != end:
            try:
                print('UPDATE %s from %s to %s' % (product_code, start, end), end="")
                future_df = QATdx.QA_fetch_get_future_min(product_code,
                                                          start=start,
                                                          end=end)
                if len(future_df) > 0:
                    future_json = future_df.to_json(orient='records')
                    col.insert_many(json.loads(future_json))
                    print(' OK!')
                else:
                    no_data_list.append(product_code)
                    print(' NO DATA FROM TDX!')
                succ_product_list.append(product_code)
            except socket.timeout as ex:
                err_timeout_product_list.append('%s: error.socket.timeout' % product_code)
                print(' TIMEOUT!')
            except Exception as ex:
                err_unknown_product_list.append('%s: %s\n %s' % (product_code, ex, future_df))
                print(' UNKNOWN_ERROR!')
        else:
            print('%s is up to date %s' % (product_code, end))
    QA_util_log_info('Update future_min completed!\nTotal succeed: %d\n Total error:%d' %
                     (len(succ_product_list),
                      len(err_timeout_product_list) + len(err_unknown_product_list)))
    if len(err_timeout_product_list) > 0:
        for each_err_log in err_timeout_product_list:
            QA_util_log_info(each_err_log)
    if len(err_unknown_product_list) > 0:
        for each_err_log in err_unknown_product_list:
            QA_util_log_info(each_err_log)
    if len(no_data_list) > 0:
        QA_util_log_info('No data from TDX: %s' % no_data_list)


if __name__ == '__main__':
    QA_SU_save_future_day()
    QA_SU_save_future_min()