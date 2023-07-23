# -*- coding: utf-8 -*-
import os
import argparse
import csv

import mysql.connector

HEADER = ["Symbol"]


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

    def export_data(self, save_dir, index_name, trade_date):
        """导出数据到文件
        Args:
            save_dir: 导出到目录
            index_name: 指数名称
            trade_date: 交易日期
        """
        # 创建目录
        if not os.path.exists(save_dir):
            os.makedirs(save_dir)

        # 从数据库导出数据
        with self.connection.cursor() as cursor:
            # 查询数据
            if index_name == 'csi100':
                query = "SELECT DISTINCT ts_code FROM ts_idx_index_weight " \
                        "WHERE index_code='000903.SH' AND trade_date='%s'" % trade_date
            elif index_name == 'csi500':
                query = "SELECT DISTINCT ts_code FROM ts_idx_index_weight " \
                        "WHERE index_code='000905.SH' AND trade_date='%s'" % trade_date
            elif index_name == 'csi1000':
                query = "SELECT DISTINCT ts_code FROM ts_idx_index_weight " \
                        "WHERE index_code='000852.SH' AND trade_date='%s'" % trade_date
            elif index_name == 'sh300':
                query = "SELECT DISTINCT ts_code FROM ts_idx_index_weight " \
                        "WHERE index_code='000300.SH' AND trade_date='%s'" % trade_date
            elif index_name == 'szzs':
                query = "SELECT DISTINCT ts_code FROM ts_idx_index_weight " \
                        "WHERE index_code = '399001.SZ' AND trade_date='%s'" % trade_date
            else:
                raise ValueError('Unknown index name')

            print(query)

            cursor.execute(query)

            with open('%s/instruments/%s.csv' % (save_dir, index_name), 'w', newline='') as csvfile:
                writer = csv.writer(csvfile, delimiter=',',
                                    quotechar='|', quoting=csv.QUOTE_MINIMAL)
                writer.writerow(HEADER)

                for row in cursor:
                    list_row = list(row)
                    writer.writerow(list_row)


if __name__ == '__main__':
    """主程序，解析参数，并执行相关的命令"""
    parser = argparse.ArgumentParser(description='获取指数里的每支股票')
    parser.add_argument('-save_dir', required=True, type=str,
                        help='Directory of the files')
    parser.add_argument('-index_name', type=str, help='index name')
    parser.add_argument('-trade_date', type=str, help='trade date')
    parser.add_argument('-host', required=True, type=str, default='127.0.0.1',
                        help='The address of database')
    parser.add_argument('-user', required=True, type=str, default='zcs',
                        help='The user name of database')
    parser.add_argument('-passwd', required=True, type=str, default='mydaydayup2023!',
                        help='The password of database')

    args = parser.parse_args()

    # 解析命令行中的参数
    print("Begin export data, save_dir: %s, index_name: %s, date: %s" % (
        args.save_dir, args.index_name, args.trade_date))

    export = ExportCodeData(args)
    export.export_data(args.save_dir, args.index_name, args.trade_date)

    print("End export data")
