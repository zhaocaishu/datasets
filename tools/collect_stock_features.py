# -*- coding: utf-8 -*-
import os
import argparse
import csv

import mysql.connector

from helpers.industry import name2id as industry_name2id

header = ["Symbol", "Date", "Open", "Close", "High", "Low", "Pre_Close", "Change", "Pct_Chg", "Volume", "AMount",
          "Turnover_rate", "Turnover_rate_f", "Volume_ratio", "Pe", "Pe_ttm", 'Pb', 'Ps', 'Ps_ttm', 'Dv_ratio',
          'Dv_ttm', 'Total_share', 'Float_share', 'Free_share', 'Total_mv', 'Circ_mv', 'Adj_factor', 'Industry_id']


class ExportCodeData(object):
    def __init__(self, args):
        self.__init_db(args.host, args.user, args.passwd)

    def __init_db(self, host, user, passwd):
        """init db, and show tables"""
        self.connection = mysql.connector.connect(host=host, user=user, passwd=passwd,
                                                  database="stock_info")

        with self.connection.cursor() as cursor:
            cursor.execute("SHOW TABLES")

            for table in cursor:
                print(table)

    def get_codes(self) -> list:
        """获取一年以上在主板上市的股票
        """
        codes = []
        query = "SELECT ts_code, industry FROM ts_basic_stock_list WHERE list_status='L' AND market='主板' " \
                "AND DATEDIFF(NOW(), DATE_FORMAT(list_date, '%Y%m%d')) >= 360"

        with self.connection.cursor() as cursor:
            cursor.execute(query)
            for code in cursor:
                codes.append(code)

        print("1年以上在主板上市的股票共计%d个" % (len(codes)))

        return codes

    def export_data(self, dir, trade_date):
        """导出数据到文件
        :param dir: 导出到目录
        :param trade_date: 交易日期为这个之后的
        """
        # 创建目录
        if not os.path.exists(dir):
            os.makedirs(dir)

        # 获取主板上市一年以上的股票代码
        codes = self.get_codes()

        # 从数据库导出数据
        with self.connection.cursor() as cursor:
            for code, industry in codes:
                # 查询数据
                query = "SELECT daily.*, daily_basic.turnover_rate, daily_basic.turnover_rate_f, " \
                        "daily_basic.volume_ratio, daily_basic.pe, daily_basic.pe_ttm, " \
                        "daily_basic.pb, daily_basic.ps, daily_basic.ps_ttm, daily_basic.dv_ratio, " \
                        "daily_basic.dv_ttm, daily_basic.total_share, daily_basic.float_share, daily_basic.free_share, " \
                        "daily_basic.total_mv, daily_basic.circ_mv, factor.adj_factor " \
                        "FROM ts_quotation_daily daily " \
                        "JOIN ts_quotation_daily_basic daily_basic ON " \
                        "daily.ts_code=daily_basic.ts_code AND " \
                        "daily.trade_date=daily_basic.trade_date " \
                        "JOIN ts_quotation_adj_factor factor ON " \
                        "daily.ts_code=factor.ts_code AND " \
                        "daily.trade_date=factor.trade_date " \
                        "WHERE daily.ts_code='%s' AND daily.trade_date >= '%s' " \
                        "LIMIT 10000" % (code, trade_date)

                print(query)

                cursor.execute(query)

                with open('%s/%s.csv' % (dir, code), 'w', newline='') as csvfile:
                    writer = csv.writer(csvfile, delimiter=',',
                                        quotechar='|', quoting=csv.QUOTE_MINIMAL)
                    writer.writerow(header)

                    for row in cursor:
                        list_row = list(row)
                        t_date = list_row[1]
                        list_row[1] = t_date[0:4] + '-' + t_date[4:6] + '-' + t_date[6:8]
                        list_row.append(industry_name2id[industry])
                        writer.writerow(list_row)


if __name__ == '__main__':
    """主程序，解析参数，并执行相关的命令"""
    parser = argparse.ArgumentParser(description='1）获取上市一年以上的股票列表；'
                                                 '2）查询这些股票自trade_date以来的数据')
    parser.add_argument('-dir', required=True, type=str,
                        help='Dir of the files')
    parser.add_argument('-trade_date', required=True, type=str,
                        help='Data after trade_date, format yyyymmdd')
    parser.add_argument('-host', required=True, type=str,
                        help='The address of database')
    parser.add_argument('-user', required=True, type=str,
                        help='The user name of database')
    parser.add_argument('-passwd', required=True, type=str,
                        help='The password of database')

    args = parser.parse_args()

    # 解析命令行中的参数，得到需要爬取的数据、日期范围
    print("Begin export data, dir: %s, trade_date: %s" % (args.dir, args.trade_date))

    export = ExportCodeData(args)
    export.export_data(os.path.join(args.dir, 'features'), args.trade_date)

    print("End export data")
