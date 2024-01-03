import concurrent
import os
from abc import abstractmethod
from concurrent.futures import as_completed

from pymysql import DatabaseError, MySQLError
from tqdm import tqdm

from base import Mysql, logger, ImportInterface


class BaseImport(ImportInterface):
    """
    导入基类
    """

    def __init__(self, source: Mysql, target: Mysql, databases: list, dumps_folder: str, max_workers=8):
        self.source = source
        self.target = target
        self.databases = databases
        self.dumps_folder = dumps_folder
        self.max_workers = max_workers

    def get_columns_dtype(self, database, table):
        """
        返回csv读取时指定的dtype
        :param database: 数据库
        :param table: 表名
        :return: 返回dtype类型对象  指定类型 例如： {'a': np.int16, 'b': np.float64}
        """
        return None

    def __create_database_if_not_exists(self, database):
        """
        自动判断是否创建目标库
        :param database: 数据库
        :return:
        """
        try:
            db_create_sql = self.source.get_database_create_sql(database)
            db_create_sql = db_create_sql.replace('CREATE DATABASE', 'CREATE DATABASE IF NOT EXISTS')
            self.target.execute_update(db_create_sql)
        except BaseException as e:
            raise DatabaseError(f'【{database}】错误: {repr(e)}')

    def __create_table_if_not_exists(self, database, table):
        """
        自动判断是否创建目标表
        :param database:
        :param table:
        :return:
        """
        try:
            create_sql = self.source.get_table_create_sql(table, database)
            create_sql = create_sql.replace('CREATE TABLE', 'CREATE TABLE IF NOT EXISTS')
            create_sql = create_sql.replace('utf8mb4_0900_ai_ci', 'utf8mb4_general_ci')
            self.target.execute_update(create_sql, database)
        except BaseException as e:
            raise MySQLError(f'create table error: 【{database}.{table}】 {repr(e)}')

    def _import_database(self, database, is_truncate_data=False):
        """
        导入指定的数据库
        :param database: 数据库
        :return:
        """

        # 创建目标数据库，如果不存在
        self.__create_database_if_not_exists(database)

        # csv所在目录
        database_folder = os.path.join(self.dumps_folder, database)
        # 源数据库表列表
        source_tables = self.source.list_tables(database=database)

        table_bar = tqdm(total=len(source_tables), desc=f'数据库【{database}】的表处理进度')
        for index, table in enumerate(source_tables):
            try:
                # 调试模式，单独只导入某一个表
                debug_table = self.single_table_for_debug()
                if debug_table and table != debug_table:
                    continue

                # 如果表不存在，则创建
                self.__create_table_if_not_exists(database, table)

                # 导入csv
                filename = os.path.join(database_folder, f'{table}.csv')
                if os.path.exists(filename):

                    index_alert_sqls = self.target.get_table_index_alert_sqls(database, table)
                    # 导入前删除索引
                    index_drop_sqls = self.target.get_table_index_drop_sql(database, table)
                    if index_drop_sqls:
                        for index_drop in index_drop_sqls:
                            self.target.execute_update(index_drop, database=database)

                    # 开始导入
                    csv_file = os.path.join(database_folder, filename)
                    self.target.from_csv_to_table(csv_file, database, table, is_truncate_data,
                                                  chunk_wrapper=self.chunk_wrapper,
                                                  dtype=self.get_columns_dtype(database, table))

                    # 导入后恢复索引
                    if index_alert_sqls:
                        for index_alert in index_alert_sqls:
                            self.target.execute_update(index_alert, database)

                    logger.info(f'【{database}.{table}】导入成功, 剩余 {index + 1}/{len(source_tables)}')
                else:
                    logger.warning(f'【{database}.{table}】无需处理, 剩余 {index + 1}/{len(source_tables)}')
            except BaseException as e:
                logger.error(f'_import_database error: {repr(e)}')
            table_bar.update(1)
        pass

    def import_parallel(self, is_truncate_data=False):
        """
        并发批量导入
        :return:
        """
        import_bar = tqdm(total=len(self.databases), desc=f'实例【{self.get_name()}】的数据库处理进度')
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            futures = []
            for db in self.databases:

                # 并发执行的导入方法
                def import_database(database):
                    try:
                        # 开始导入
                        self._import_database(database, is_truncate_data=is_truncate_data)
                    except BaseException as e:
                        logger.error(f'【{database}】【导入失败】: {repr(e)}')

                # 提交并发执行
                future = pool.submit(import_database, db)
                futures.append(future)
            for future in as_completed(futures):  # 并发执行
                import_bar.update(1)
