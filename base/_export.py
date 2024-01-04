import concurrent
import os
import shutil
from concurrent.futures import as_completed

from base._utils import logger
from base._sink import Mysql
from base._interface import ExportInterface


class BaseExport(ExportInterface):
    """
    导出到csv基类
    """

    def __init__(self, source: Mysql, databases: list, dumps_folder: str, max_workers=4):
        self.source = source
        self.databases = databases
        self.dumps_folder = dumps_folder
        self.max_workers = max_workers

    def _export_database(self, database, ent_code):
        """
        导出源的指定数据库所有表
        :param database:
        :param ent_code:
        :return:
        """
        # 先删除数据库目录，如果存在的话
        database_file = os.path.join(self.dumps_folder, database)
        if os.path.exists(database_file):
            shutil.rmtree(database_file)

        source_tables = self.source.list_tables(database)
        for index, source_table in enumerate(source_tables):

            # 调试模式，单独只导入某一个表
            debug_table = self.single_table_for_debug()
            if debug_table and source_table != debug_table:
                continue

            # 匹配表过滤器，如果返回true则继续导出，否则跳过当前表继续下一个
            if self.table_data_match_filter:
                matcher = self.table_data_match_filter(database, source_table)
                if not matcher:
                    continue

            logger.info(f'    【导出表 {database}.{source_table} {index + 1}/{len(source_tables)}】。。。')
            csv_file = os.path.join(self.dumps_folder, database, f'{source_table}.csv')
            # csv存在的话先删除文件,几乎没有这个情况。因为上面先删除库目录的。
            if os.path.exists(csv_file):
                os.remove(csv_file)
            # csv目录不存在的话先创建
            if not os.path.exists(os.path.dirname(csv_file)):
                os.makedirs(os.path.dirname(csv_file))

            # 判断表是否包含ent_code字段
            exist_ent_code_column = self.source.exists_table_column(database, source_table, 'ent_code')

            # 不包含ent_code字段，则导出全表， 否则导出ent_code条件内数据
            if not exist_ent_code_column:
                self.source.from_table_to_csv(database, source_table,
                                              csv_file=csv_file)
            else:
                count_sql = f"/** 导出数量 **/ select count(0) from `{database}`.`{source_table}` where ent_code = '{ent_code}'"
                query_sql = f"/** 导出数据 **/ select * from `{database}`.`{source_table}` where ent_code = '{ent_code}'"

                def chunk_callback(item):
                    # 替换bit类型的 b'\x00' 值为0
                    item = item.map(lambda x: x[0] if type(x) is bytes else x)
                    return item

                self.source.from_sql_to_csv(count_sql, query_sql, database=database, csv_file=csv_file,
                                            chunk_callback=chunk_callback)

    def export_parallel(self, ent_code):
        """
        导出源库指定数据库列表的所有表,并行执行
        :param ent_code:
        :return:
        """
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            futures = []
            for index, db in enumerate(self.databases):

                # 导出入口方法
                def export_database(database):
                    try:
                        logger.info(f'【导出库 {database} {index + 1}/{len(self.databases)}】。。。')
                        self._export_database(database, ent_code)
                    except BaseException as e:
                        logger.error(f"【导出失败】{repr(e)}")

                # 并发执行
                future = pool.submit(export_database, db)
                futures.append(future)
            for future in as_completed(futures):  # 并发执行
                logger.info(f'【导出成功 {db} {index + 1}/{len(self.databases)}】')
