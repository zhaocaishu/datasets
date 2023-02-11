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

    def export_data(self, dir):
        """导出数据到文件
        :param dir: 导出到目录
        """
        # 创建目录
        if not os.path.exists(dir):
            os.makedirs(dir)

        # 从数据库导出数据
        with self.connection.cursor() as cursor:
            # 查询数据
            query = "SELECT DISTINCT date_format(trade_date, '%Y-%m-%d') " \
                    "FROM ts_quotation_daily order by trade_date ASC"

            print(query)

            cursor.execute(query)

            with open('%s/calendars/days.txt' % dir, 'w') as fp:
                for row in cursor:
                    list_row = list(row)
                    fp.write(list_row[0] + '\n')


if __name__ == '__main__':
    """主程序，解析参数，并执行相关的命令"""
    parser = argparse.ArgumentParser(description='获取全部交易日期')
    parser.add_argument('-dir', required=True, type=str,
                        help='Dir of the files')
    parser.add_argument('-host', required=True, type=str,
                        help='The address of database')
    parser.add_argument('-user', required=True, type=str,
                        help='The user name of database')
    parser.add_argument('-passwd', required=True, type=str,
                        help='The password of database')

    args = parser.parse_args()

    # 解析命令行中的参数，得到全部交易日期
    print("Begin export data, dir: %s" % args.dir)

    export = ExportCodeData(args)
    export.export_data(args.dir)

    print("End export data")
