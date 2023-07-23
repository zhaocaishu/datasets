# -*- coding: utf-8 -*-
import os
import argparse
import csv

import mysql.connector

from helpers.industry import name2id as industry_name2id

META_HEADER = ['Code', 'Ind_class', 'List_date']

FEATURE_HEADER = ["Symbol", "Date", "Open", "Close", "High", "Low", "Pre_Close", "Change", "Pct_Chg", "Volume",
                  "AMount", "Turnover_rate", "Turnover_rate_f", "Volume_ratio", "Pe", "Pe_ttm", 'Pb', 'Ps', 'Ps_ttm',
                  'Dv_ratio', 'Dv_ttm', 'Total_share', 'Float_share', 'Free_share', 'Total_mv', 'Circ_mv', 'Adj_factor',
                  'Ind_class']


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
        """获取全部上市的股票
        """
        codes = []
        query = "SELECT stock.ts_code, industry.industry_name_lv1, DATE_FORMAT(stock.list_date, '%Y-%m-%d') " \
                "FROM ts_basic_stock_list stock JOIN ts_idx_sw_member industry ON stock.ts_code = industry.ts_code " \
                "COLLATE utf8mb4_unicode_ci WHERE stock.market in ('主板', '中小板', '创业板', '科创板') " \
                "AND stock.list_status = 'L'"

        with self.connection.cursor() as cursor:
            cursor.execute(query)
            for code in cursor:
                codes.append(code)

        print("合计%d个股票" % (len(codes)))

        return codes

    def export_data(self, save_dir):
        """导出数据到文件
        Args:
            save_dir: 导出到目录
        """
        # 创建目录
        if not os.path.exists(save_dir):
            os.makedirs(save_dir)

        # 获取上市的全部股票代码
        codes = self.get_codes()

        with open('%s/meta.csv' % save_dir, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile, delimiter=',',
                                quotechar='|', quoting=csv.QUOTE_MINIMAL)
            writer.writerow(META_HEADER)

            for code, industry, list_date in codes:
                writer.writerow([code, industry, list_date])

        # 从数据库导出数据
        with self.connection.cursor() as cursor:
            for code, industry, list_date in codes:
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
                        "WHERE daily.ts_code='%s' LIMIT 50000"

                print(query)

                cursor.execute(query)

                with open('%s/%s.csv' % (save_dir, code), 'w', newline='') as csvfile:
                    writer = csv.writer(csvfile, delimiter=',',
                                        quotechar='|', quoting=csv.QUOTE_MINIMAL)
                    writer.writerow(FEATURE_HEADER)

                    for row in cursor:
                        list_row = list(row)
                        t_date = list_row[1]
                        list_row[1] = t_date[0:4] + '-' + t_date[4:6] + '-' + t_date[6:8]
                        list_row.append(industry_name2id[industry])
                        writer.writerow(list_row)


if __name__ == '__main__':
    """主程序，解析参数，并执行相关的命令"""
    parser = argparse.ArgumentParser(description='查询并保存全部股票数据')
    parser.add_argument('-save_dir', required=True, type=str,
                        help='Directory of the saved files')
    parser.add_argument('-host', required=True, type=str, default='127.0.0.1',
                        help='The address of database')
    parser.add_argument('-user', required=True, type=str, default='zcs',
                        help='The user name of database')
    parser.add_argument('-passwd', required=True, type=str, default='mydaydayup2023!',
                        help='The password of database')

    args = parser.parse_args()

    # 解析命令行中的参数，得到需要爬取的数据、日期范围
    print("Begin export data, save directory: %s" % args.save_dir)

    export = ExportCodeData(args)
    export.export_data(os.path.join(args.save_dir, 'features'))

    print("End export data")
