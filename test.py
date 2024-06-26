import os
import unittest

from base import *
from rds01 import Rds01
from rds02 import Rds02


class TestTable(unittest.TestCase):

    def test_export(self):
        """
        导出线上数据指定账套内容
        :return:
        """
        ent_code = '638334323'
        rds_list = [Rds01(), Rds02()]
        for rds in rds_list:
            logger.info(f'【导出csv {rds.get_name()}】。。。')
            rds.export_parallel(ent_code)
        pass

    def test_import(self):
        """
        导入csv数据到uat环境
        :return:
        """
        # 是否清空目标表数据
        is_truncate_data = True
        rds01 = Rds01()
        rds02 = Rds02()
        rds_list = [rds01, rds02]
        for rds in rds_list:
            logger.info(f'【开始导入csv {rds.get_name()}】。。。')
            rds.import_parallel(is_truncate_data)
        pass

    # 读取sql，测试转换异常字段类型
    def test_read_table(self):
        csv_files = [
            '/Users/liuyuhua/Downloads/setting_from_table.csv',
            '/Users/liuyuhua/Downloads/setting_from_sql.csv'
        ]
        for f in csv_files:
            if os.path.exists(f):
                os.remove(f)

        def chunk_callback(chunk):
            print(chunk)
            return chunk

        count_sql = 'select count(0) from data_authority.setting'
        query_sql = 'select * from data_authority.setting'

        # rds01 mysql连接
        self.rds_host = config.get('source_mysql', 'rds_01_host')
        self.rds_port = config.get('source_mysql', 'rds_01_port')
        self.rds_user = config.get('source_mysql', 'rds_01_user')
        self.rds_pass = config.get('source_mysql', 'rds_01_pass')

        source_rds01 = Mysql(self.rds_host, self.rds_port, self.rds_user, self.rds_pass)
        # source_rds01.from_table_to_csv('data_authority', 'setting', csv_files[0],
        #                                chunk_callback=chunk_callback)
        source_rds01.from_sql_to_csv(count_sql, query_sql, csv_files[1],
                                     database='data_authority',
                                     chunk_callback=chunk_callback)
        pass

    # 测试读取csv
    def test_csv(self):
        dumps_folder = config.get('global', 'dumps_folder')
        csv_file = os.path.join(dumps_folder, 'data_authority', 'setting.csv')
        csv = Csv()
        chunks = csv.get_chunks_from_csv(csv_file)
        for index, item in enumerate(chunks):
            print(item)
