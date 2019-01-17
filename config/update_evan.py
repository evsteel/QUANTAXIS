from QUANTAXIS.QASU.main import (QA_SU_save_etf_day, QA_SU_save_etf_min,
                                 QA_SU_save_financialfiles,
                                 QA_SU_save_index_day, QA_SU_save_index_min,
                                 QA_SU_save_stock_block, QA_SU_save_stock_day,
                                 QA_SU_save_stock_info,
                                 QA_SU_save_stock_info_tushare,
                                 QA_SU_save_stock_list, QA_SU_save_stock_min,
                                 QA_SU_save_stock_xdxr,QA_SU_save_report_calendar_day,
                                 QA_SU_save_report_calendar_his,QA_SU_save_stock_divyield_day,
                                 QA_SU_save_stock_divyield_his)
from QUANTAXIS.QASU.save_binance import (QA_SU_save_binance,
                                         QA_SU_save_binance_1day,
                                         QA_SU_save_binance_1hour,
                                         QA_SU_save_binance_1min,
                                         QA_SU_save_binance_symbol)
from QUANTAXIS.QASU.save_bitmex import QA_SU_save_bitmex
from QUANTAXIS.QASU.main import (QA_SU_save_etf_day, QA_SU_save_etf_min,
                                 QA_SU_save_financialfiles,
                                 QA_SU_save_index_day, QA_SU_save_index_min,
                                 QA_SU_save_stock_block, QA_SU_save_stock_day,
                                 QA_SU_save_stock_info,
                                 QA_SU_save_stock_info_tushare,
                                 QA_SU_save_stock_list, QA_SU_save_stock_min,
                                 QA_SU_save_stock_xdxr,QA_SU_save_future_day,QA_SU_save_future_day_all,
                                 QA_SU_save_future_min,QA_SU_save_future_min_all)

QA_SU_save_stock_day('tdx')
QA_SU_save_stock_xdxr('tdx')
QA_SU_save_stock_min('tdx')
QA_SU_save_index_day('tdx')
QA_SU_save_index_min('tdx')
QA_SU_save_etf_day('tdx')
QA_SU_save_etf_min('tdx')
QA_SU_save_stock_list('tdx')
QA_SU_save_stock_block('tdx')
QA_SU_save_stock_info('tdx')
QA_SU_save_future_min_all('tdx')
QA_SU_save_future_day_all('tdx')
QA_SU_save_binance('1d')
QA_SU_save_binance('1h')
QA_SU_save_binance('1m')
QA_SU_save_bitmex()