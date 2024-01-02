import concurrent
import os
import shutil
from abc import abstractmethod
from concurrent.futures import as_completed

from pymysql import DatabaseError, MySQLError
from tqdm import tqdm

from sink import Mysql
from utils import logger


class BaseExport:
    """
    导出到csv基类
    """

    def __init__(self, source: Mysql, databases: list, dumps_folder: str, max_workers=4):
        self.source = source
        self.databases = databases
        self.dumps_folder = dumps_folder
        self.max_workers = max_workers

    @abstractmethod
    def get_name(self):
        return ''

    @abstractmethod
    def is_skiped(self):
        """
        是否忽略当前导出实例
        :return:
        """
        return False

    def table_match_filter(self, database, table):
        """
        匹配表过滤器，如果返回true则继续导出，否则跳过当前表继续下一个
        :param database: 数据库
        :param table: 表名
        :return: True:处理当前表  False:不处理当前表
        """
        return True

    def single_table_for_debug(self):
        """
        返回单独某一个表，调试状态使用
        :return:
        """
        return None

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
            if self.table_match_filter:
                matcher = self.table_match_filter(database, source_table)
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

            # ###########################################################
            # sql_create_file = os.path.join(self.dumps_folder, database, f'{source_table}.sql')
            # # sql存在的话先删除文件
            # if os.path.exists(sql_create_file):
            #     os.remove(sql_create_file)
            # # sql目录不存在的话先创建
            # if not os.path.exists(os.path.dirname(sql_create_file)):
            #     os.makedirs(os.path.dirname(sql_create_file))
            #
            # # 创建表语句写入sql文件
            # create_sql = self.source.get_table_create_sql(source_table, database=database)
            # create_sql = create_sql.replace('CREATE TABLE', 'CREATE TABLE IF NOT EXISTS')
            # with open(sql_create_file, "w") as file:
            #     file.write(create_sql)
            # ############################################################

            # 判断表是否包含ent_code字段
            exist_ent_code_column = self.source.exists_table_column(database, source_table, 'ent_code')

            # 不包含ent_code字段，则导出全表， 否则导出ent_code条件内数据
            if not exist_ent_code_column:
                self.source.from_table_to_csv(database, source_table,
                                              csv_file=csv_file)
            else:
                count_sql = f"/** 导出数量 **/ select count(0) from `{database}`.`{source_table}` where ent_code = '{ent_code}'"
                query_sql = f"/** 导出数据 **/ select * from `{database}`.`{source_table}` where ent_code = '{ent_code}'"
                self.source.from_sql_to_csv(count_sql, query_sql, database=database, csv_file=csv_file)

    def export_parallel(self, ent_code):
        """
        导出源库指定数据库列表的所有表,并行执行
        :param ent_code:
        :return:
        """

        if self.is_skiped():
            return
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


class BaseImport:
    """
    导入基类
    """

    def __init__(self, source: Mysql, target: Mysql, databases: list, dumps_folder: str, max_workers=8):
        self.source = source
        self.target = target
        self.databases = databases
        self.dumps_folder = dumps_folder
        self.max_workers = max_workers

    @abstractmethod
    def get_name(self):
        return ''

    def single_table_for_debug(self):
        """
        返回单独某一个表，调试状态使用
        :return:
        """
        return None

    def chunk_wrapper(self, df, database, table):
        """
        读取到csv后，进行的二次包装处理，比如过滤数据
        :param df: 读取的csv文件到pandas对象
        :param database: 要导入的数据库
        :param table: 要导入到的表
        :return: pandas DataFrame 对象
        """
        return df

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

    def _import_database(self, database, is_renew_database=False, is_truncate_data=False):
        """
        导入指定的数据库
        :param database: 数据库
        :return:
        """

        # 重建数据库的话，就先删除
        if is_renew_database:
            self.target.execute_update(f'drop database if exists {database}')

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

    def import_parallel(self, is_renew_database=False, is_truncate_data=False):
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
                        self._import_database(database, is_renew_database=is_renew_database,
                                              is_truncate_data=is_truncate_data)
                    except BaseException as e:
                        logger.error(f'【{database}】【导入失败】: {repr(e)}')

                # 提交并发执行
                future = pool.submit(import_database, db)
                futures.append(future)
            for future in as_completed(futures):  # 并发执行
                import_bar.update(1)
