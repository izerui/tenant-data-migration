import concurrent
import os
from concurrent.futures import as_completed

from pandas import DataFrame
from pymysql import DatabaseError, MySQLError
from tqdm import tqdm

from base._export import ExportInterface
from base._import import ImportInterface
from base._sink import Mysql
from base._utils import logger


class BaseSync(ExportInterface, ImportInterface):
    """
    同步基类
    """

    def __init__(self, source: Mysql, target: Mysql, databases: list, max_workers=8):
        self.source = source
        self.target = target
        self.databases = databases
        self.max_workers = max_workers

    def __create_database_if_not_exists(self, database):
        """
        自动判断是否创建目标库
        :param database: 数据库
        :return:
        """
        try:
            if not self.target.exists_database(database):
                db_create_sql = self.source.get_database_create_sql(database)
                db_create_sql = db_create_sql.replace('CREATE DATABASE', 'CREATE DATABASE IF NOT EXISTS')
                self.target.execute_update(db_create_sql)
        except BaseException as e:
            raise DatabaseError(f'【{database}】错误: {repr(e)}')

    def __create_database_tables_if_not_exists(self, database, tables):
        """
        创建目标表
        :param database:
        :param tables:
        :return:
        """
        try:
            # 如果文件存在则使用sql文件创建
            create_tables_sql_file = os.path.join('sqls', 'create', f'{database}.sql')
            if os.path.exists(create_tables_sql_file):
                self.target.import_sql_file(create_tables_sql_file, database)
            else:
                # 重建目标表
                table_create_sqls = []
                for table in tables:
                    if not self.target.exists_table(database, table):
                        create_sql = self.source.get_table_create_sql(table, database)
                        create_sql = create_sql.replace('CREATE TABLE', 'CREATE TABLE IF NOT EXISTS')
                        create_sql = create_sql.replace('utf8mb4_0900_ai_ci', 'utf8mb4_general_ci')
                        create_sql = create_sql.replace('ROW_FORMAT=COMPACT', '')
                        table_create_sqls.append(create_sql)
                if len(table_create_sqls) > 0:
                    self.target.execute_updates(table_create_sqls, database, f'【{database}】创建目标表...')
        except BaseException as e:
            raise MySQLError(f'create error: 【{database}】 {repr(e)}')

    def return_before_handle_data(self, database, table):
        """
        数据前置处理器
        :param database:
        :param table:
        :return:
        """
        # 记录索引
        index_alert_sqls = self.target.get_table_index_alert_sqls(database, table)
        # 导入前删除索引
        index_drop_sqls = self.target.get_table_index_drop_sql(database, table)
        if index_drop_sqls:
            for index_drop in index_drop_sqls:
                self.target.execute_update(index_drop, database=database)
        return index_alert_sqls

    def after_handle_data(self, database, table, before_return_result):
        # 导入后恢复索引
        if before_return_result:
            for index_alert in before_return_result:
                self.target.execute_update(index_alert, database)
        pass

    def _sync_database_table(self, database, table, ent_codes, test_data=False, sync_platform_data=True,
                             sync_tenant_data=True):
        """
        同步源库下的表数据到目标库下
        :param database: 数据库
        :param table: 表
        :param ent_codes: 账套编号列表
        :param test_data: 测试模式 只同步前10条记录
        :param sync_platform_data: 是否同步平台表数据
        :param sync_tenant_data: 是否同步租户数据
        :return:
        """
        # 调试模式，单独只导入某一个表
        debug_table = self.single_table_for_debug()
        if debug_table and table != debug_table:
            return

        # 匹配表过滤器，如果返回true则继续导出，否则跳过当前表继续下一个
        if self.table_data_match_filter:
            matcher = self.table_data_match_filter(database, table)
            if not matcher:
                return

        # 判断表是否包含ent_code字段
        exists_ent_code_column = self.source.exists_table_column(database, table, 'ent_code')

        # 如果不需要同步平台表的数据
        if not sync_platform_data and not exists_ent_code_column:
            return

        # 如果不需要同步租户数据
        if not sync_tenant_data and exists_ent_code_column:
            return

        # 前置处理器获取目标表的索引
        index_alert_sqls = self.return_before_handle_data(database, table)

        # 读取到数据分批写入到目标表
        def from_chunk_to_target_table(chunk: DataFrame):
            chunk = self.chunk_wrapper(chunk, database, table, True)
            if len(chunk) > 0:
                chunk.to_sql(table, schema=database, con=self.target.get_engine(), if_exists='append',
                             index=False)
            pass

        # 测试的话，只同步前10条记录
        if test_data:
            query_sql = f"/** 导出数据 **/ select * from `{database}`.`{table}` limit 10"
            self.source.from_sql_to_call_no_processor(query_sql, from_chunk_to_target_table, database=database)
        else:
            # 不包含ent_code字段，则导出全表， 否则导出ent_code条件内数据
            if not exists_ent_code_column:
                self.target.execute_update(f'truncate table `{database}`.`{table}`', database=database)
                self.source.from_table_to_call_no_processor(database, table, from_chunk_to_target_table)
            else:
                for ent_code in ent_codes:
                    self.target.execute_update(f"delete from `{database}`.`{table}` where ent_code = '{ent_code}'",
                                               database=database)
                    query_sql = f"/** 导出数据 **/ select * from `{database}`.`{table}` where ent_code = '{ent_code}'"
                    self.source.from_sql_to_call_no_processor(query_sql, from_chunk_to_target_table, database=database)

        self.after_handle_data(database, table, index_alert_sqls)
        pass

    def sync_parallel(self, ent_codes, test_data=False, drop_database=False, sync_platform_data=True,
                      sync_tenant_data=True):
        """
        并行同步实例下的多个数据库表数据
        :param ent_codes: 账套列表
        :param test_data: 测试模式 只同步前10条记录
        :param drop_database: 是否删除数据库
        :param sync_platform_data: 是否同步平台表数据
        :param sync_tenant_data: 是否同步租户数据
        :return:
        """
        tbl_count = 0
        db_map = {}
        for database in self.databases:
            # 列出源库的所有数据表
            tables = self.source.list_tables(database=database)
            logger.info(f'{database} -> {tables}')
            tbl_count += len(tables)
            db_map[f'{database}'] = tables

        for database in db_map.keys():
            # 删除数据库
            if drop_database:
                # 先重建目标数据库结构
                logger.info(f'【{database}】删除重建。。。')
                self.target.execute_update(f'drop database if exists {database}')
            # 如果库或者表不存在则创建
            self.__create_database_if_not_exists(database)
            self.__create_database_tables_if_not_exists(database, db_map[database])

        # 同步数据
        import_bar = tqdm(total=tbl_count, desc=f'实例【{self.get_name()}】的数据处理进度')
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            futures = []

            # 同步入口方法
            def sync_database(database, table):
                try:
                    # 开始同步数据
                    self._sync_database_table(database, table, ent_codes, test_data=test_data,
                                              sync_platform_data=sync_platform_data, sync_tenant_data=sync_tenant_data)
                except BaseException as e:
                    logger.error(f'\r\t【{database}.{table} 表同步失败】{repr(e)}')

            # 循环所有表，添加同步任务
            for key in db_map.keys():
                tables = db_map[key]
                for table in tables:
                    # 添加任务，并发执行
                    future = pool.submit(sync_database, key, table)
                    futures.append(future)
            for future in as_completed(futures):  # 并发执行
                import_bar.update(1)
