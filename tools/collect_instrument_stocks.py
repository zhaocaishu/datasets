# -*- coding: utf-8 -*-
import os
import argparse

import mysql.connector


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

    def export_data(self, dir, instruments, trade_date):
        """导出数据到文件
        :param dir: 导出到目录
        :param instruments: 指数名称
        :param trade_date: 交易日期
        """
        # 创建目录
        if not os.path.exists(dir):
            os.makedirs(dir)

        # 从数据库导出数据
        with self.connection.cursor() as cursor:
            # 查询数据
            if instruments == 'sh300':
                query = "SELECT DISTINCT ts_code FROM ts_idx_index_weight " \
                        "WHERE index_code='000300.SH' AND trade_date='%s'" % trade_date
            elif instruments == 'csi1000':
                query = "SELECT DISTINCT ts_code FROM ts_idx_index_weight " \
                        "WHERE index_code='000852.SH' AND trade_date='%s'" % trade_date
            elif instruments == 'csi500':
                query = "SELECT DISTINCT ts_code FROM ts_idx_index_weight " \
                        "WHERE index_code='000905.SH' AND trade_date='%s'" % trade_date
            elif instruments == 'all':
                query = "SELECT DISTINCT ts_code FROM ts_idx_index_weight " \
                        "WHERE index_code in ('000002.SH', '399001.SZ', '000300.SH', '000852.SH', '000905.SH') " \
                        "AND trade_date='%s'" % trade_date
            else:
                raise ValueError('Unknown instruments name')

            print(query)

            cursor.execute(query)

            with open('%s/instruments/%s.txt' % (dir, instruments), 'w') as fp:
                for row in cursor:
                    list_row = list(row)
                    fp.write(list_row[0] + '\t' + list_row[1] + '\t' + list_row[2] + '\n')


if __name__ == '__main__':
    """主程序，解析参数，并执行相关的命令"""
    parser = argparse.ArgumentParser(description='获取指数里每个股票的起止时间')
    parser.add_argument('-dir', required=True, type=str,
                        help='Dir of the files')
    parser.add_argument('-instruments', type=str, help='instruments name')
    parser.add_argument('-trade_date', type=str, help='trade date')
    parser.add_argument('-host', required=True, type=str,
                        help='The address of database')
    parser.add_argument('-user', required=True, type=str,
                        help='The user name of database')
    parser.add_argument('-passwd', required=True, type=str,
                        help='The password of database')

    args = parser.parse_args()

    # 解析命令行中的参数
    print("Begin export data, dir: %s, instruments: %s, date: %s" % (args.dir, args.instruments, args.trade_date))

    export = ExportCodeData(args)
    export.export_data(args.dir, args.instruments, args.trade_date)

    print("End export data")
