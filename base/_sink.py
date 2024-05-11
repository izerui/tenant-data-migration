import os
import platform
from typing import Iterator, Callable, Generator

import pandas as pd
import sqlalchemy.engine.cursor
from pandas import DataFrame
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.engine import Engine
from tqdm import tqdm

from base._utils import logger, execute_command

if platform.system() == 'Windows':
    mysqlpump_file = os.path.join('mysql-client', 'win', 'x64', 'mysqlpump.exe')
    mysqldump_file = os.path.join('mysql-client', 'win', 'x64', 'mysqldump.exe')
    mysql_file = os.path.join('mysql-client', 'win', 'x64', 'mysql.exe')
elif platform.system() == 'Darwin':
    mysqlpump_file = os.path.join('mysql-client', 'mac', 'arm64', 'mysqlpump')
    mysqldump_file = os.path.join('mysql-client', 'mac', 'arm64', 'mysqldump')
    mysql_file = os.path.join('mysql-client', 'mac', 'arm64', 'mysql')
else:
    raise BaseException('暂不支持')


class Csv:
    """
    导入导出类
    """

    def get_dataframe_from_csv(self, csv_file, dtype=None):
        """
        从csv读取数据到pandas
        :param csv_file: csv文件
        :return:
        """
        if not os.path.isfile(csv_file):
            raise FileExistsError(f'{csv_file}文件不存在')
        dataframe = pd.read_csv(filepath_or_buffer=csv_file, dtype=dtype)
        return dataframe

    def get_chunks_from_csv(self, csv_file, chunksize=10000, dtype=None) -> Generator:
        """
        从csv读取数据到pandas
        :param csv_file: csv文件
        :param chunksize: 每批次读取数量
        :return:
        """
        if not os.path.isfile(csv_file):
            raise FileExistsError(f'{csv_file}文件不存在')
        chunks = pd.read_csv(filepath_or_buffer=csv_file, chunksize=chunksize, low_memory=False, dtype=dtype)
        return chunks


class Mysql:
    """
    mysql操作基础类
    """

    def __init__(self, host: str, port: str, user: str, password: str):
        super().__init__()
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        url = f'mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/mysql'
        self.engine = create_engine(url, echo_pool=True, pool_size=20)

    def get_engine(self) -> Engine:
        """
        获取数据库操作engine对象
        :return:
        """
        # 如果未指定数据库，返回默认连接
        return self.engine

    def execute_query(self, sql, database=None) -> sqlalchemy.engine.cursor.CursorResult:
        """
        执行sql语句并返回指针结果
        :param sql: sql语句
        :return: sqlalchemy.engine.cursor.CursorResult
        """
        with self.get_engine().connect() as conn:
            if database:
                conn.execute(text(f'use `{database}`;'))
            result = conn.execute(text(sql))
        return result

    def execute_update(self, sql, database=None, parameters=None):
        """
        执行更新或者插入语句, 支持通配符
        :param sql: sql语句
        :return: sqlalchemy.engine.cursor.CursorResult
        """
        conn = self.get_engine().connect()
        with conn.begin() as txn:
            if database:
                conn.execute(text(f'use `{database}`;'))
            conn.execute(text(sql), parameters)
            txn.commit()
        conn.close()

    def execute_updates(self, sqls, database=None, desc=None):
        """
        执行更新或者插入语句集合, 支持通配符
        :param sqls: sql语句集合
        :return: sqlalchemy.engine.cursor.CursorResult
        """
        sql_bar = tqdm(total=len(sqls), desc=desc)
        conn = self.get_engine().connect()
        with conn.begin() as txn:
            if database:
                conn.execute(text(f'use `{database}`;'))
            for sql in sqls:
                conn.execute(text(sql), None)
                sql_bar.update(1)
            txn.commit()
        conn.close()

    def get_dataframe_chunks_from_table(self, database, table, chunksize=100000) -> Iterator[DataFrame]:
        """
        从mysql中读取表数据到 dataframe generator
        :param database: 数据库名
        :param table: 表名
        :param chunksize: 每批读取数量
        :return:
        """
        # 使用 SQL 查询语句获取数据，并将结果存储到 DataFrame 对象中
        # chunks = pd.read_sql(f'SELECT * FROM `{database}`.`{table}`', con=self.engine, chunksize=chunksize)
        chunks = pd.read_sql_table(table_name=table, con=self.get_engine(), schema=database,
                                   chunksize=chunksize)
        return chunks

    def get_dataframe_all_from_table(self, table) -> DataFrame:
        """
        从mysql中读取表数据到 dataframe generator
        :param table: 表名
        :return:
        """
        # 使用 SQL 查询语句获取数据，并将结果存储到 DataFrame 对象中
        dataframe = pd.read_sql(f'SELECT * FROM {table}', con=self.get_engine())
        return dataframe

    def get_dataframe_from_sql(self, sql) -> DataFrame:
        """
        从mysql中读取表数据到 dataframe generator
        :param sql: 查询语句
        :return:
        """
        # 使用 SQL 查询语句获取数据，并将结果存储到 DataFrame 对象中
        dataframe = pd.read_sql(sql, con=self.get_engine())
        # 替换bit类型的 b'\x00' 值为0
        dataframe = dataframe.map(lambda x: x[0] if type(x) is bytes else x)
        return dataframe

    def get_dataframe_chunks_from_sql(self, sql, chunksize=100000) -> Iterator[DataFrame]:
        """
        从mysql中读取表数据到 dataframe generator
        :param sql: 查询语句
        :param chunksize: 每批读取数量
        :return:
        """
        # 使用 SQL 查询语句获取数据，并将结果存储到 DataFrame 对象中
        chunks = pd.read_sql(sql, con=self.get_engine(), chunksize=chunksize)
        return chunks

    def from_table_to_csv(self, database, table, csv_file, chunk_callback=None):
        """
        从数据表导出到csv文件
        :param database: 数据库名
        :param table: 数据库表名
        :param csv_file: csv文件
        :return:
        """
        chunksize = 100000
        count = self.execute_query(f'SELECT count(0) FROM `{database}`.`{table}`').scalar()
        chunks = self.get_dataframe_chunks_from_table(database, table, chunksize=chunksize)
        # 显示进度
        chunks = tqdm(chunks, total=count / chunksize)
        for index, item in enumerate(chunks):
            # log.info(f'导出表数据进度: {count}')
            if chunk_callback:
                item = chunk_callback(item)
            if index > 0:
                # 追加内容
                item.to_csv(csv_file, mode='a', index=False, header=False, encoding='utf-8')
            else:
                # 将 DataFrame 对象写入 csv 文件中
                item.to_csv(csv_file, index=False, encoding='utf-8')

    def from_sql_to_csv(self, count_sql, query_sql, csv_file, database=None, chunksize=10000, chunk_callback=None):
        """
        从数据表导出到csv文件
        :param count_sql: count查询
        :param query_sql: select查询
        :param csv_file: csv文件
        :return:
        """
        count = self.execute_query(count_sql, database=database).scalar()
        chunks = self.get_dataframe_chunks_from_sql(query_sql, chunksize=chunksize)
        # 显示进度
        chunks = tqdm(chunks, total=count / chunksize)
        for index, item in enumerate(chunks):
            if len(item) == 0:
                continue
            # log.info(f'导出表数据进度: {count}')
            if chunk_callback:
                item = chunk_callback(item)
            if index > 0:
                # 追加内容
                item.to_csv(csv_file, mode='a', index=False, header=False, encoding='utf-8')
            else:
                # 创建文件并将 DataFrame 对象写入 csv 文件中
                item.to_csv(csv_file, index=False, encoding='utf-8')

    def from_table_to_call_no_processor(self, database, table, chunk_call, chunksize=10000):
        """
        从数据表导出到csv文件
        :param table: 数据库表名
        :return:
        """
        chunks = self.get_dataframe_chunks_from_table(database, table, chunksize=chunksize)
        for index, item in enumerate(chunks):
            chunk_call(item)

    def from_table_to_call(self, database, table, chunk_call, chunksize=10000):
        """
        从数据表导出到csv文件
        :param table: 数据库表名
        :return:
        """
        count_query = f'SELECT count(0) FROM `{database}`.`{table}`'
        # print(f'执行查询条目数: {count_query}')
        count = self.execute_query(count_query).scalar()
        # print(f'共 {count} 条记录，开始读取数据...')
        chunks = self.get_dataframe_chunks_from_table(database, table, chunksize=chunksize)
        # 显示进度
        chunks = tqdm(chunks, total=count / chunksize, desc=f'{table} 表from_table_to_call数据处理进度')
        for index, item in enumerate(chunks):
            chunk_call(item)

    def from_sql_to_call_no_processor(self, query_sql, chunk_call, database=None, chunksize=10000):
        """
        从数据表导出到csv文件,不显示进度条
        :param table: 数据库表名
        :return:
        """
        chunks = self.get_dataframe_chunks_from_sql(query_sql, chunksize=chunksize)
        for index, item in enumerate(chunks):
            chunk_call(item)

    def from_sql_to_call(self, count_sql, query_sql, chunk_call, database=None, chunksize=10000):
        """
        从数据表导出到csv文件
        :param table: 数据库表名
        :return:
        """
        # print(f'【执行查询条目数】: \r\n {count_sql}')
        count = self.execute_query(count_sql).scalar()
        # print(f'【条目数】共 {count} 条记录，开始读取数据...')
        chunks = self.get_dataframe_chunks_from_sql(query_sql, chunksize=chunksize)
        # 显示进度
        chunks = tqdm(chunks, total=count / chunksize, desc=f'【from_sql_to_call数据处理进度】')
        for index, item in enumerate(chunks):
            # 替换bit类型的 b'\x00' 值为0
            # item = item.map(lambda x: x[0] if type(x) is bytes else x)
            chunk_call(item)

    def from_csv_to_table(self, csv_file: str, database: str, table: str, is_truncate_data: bool,
                          chunk_wrapper: Callable = None,
                          dtype=None):
        """
        从csv文件批量导入到数据表
        :param csv_file: csv文件
        :param database: 数据库名
        :param table: 数据库表名, 如果表存在则清空表数据
        :param is_truncate_data: 是否清空数据
        :param chunk_wrapper: df对象包装过滤器 function(df, database, table) -> df
        :param dtype: 指定类型 例如： {'a': np.int16, 'b': np.float64}
        :return:
        """
        # with self.engine.connect() as conn:
        #     has_table = self.engine.dialect.has_table(conn, f"{table}", schema=database)
        try:
            if is_truncate_data:
                self.execute_update(f'truncate table `{database}`.`{table}`')
            csv = Csv()
            chunks = csv.get_chunks_from_csv(csv_file, dtype=dtype)
            for index, item in enumerate(chunks):
                if chunk_wrapper:
                    item = chunk_wrapper(item, database, table)
                item.to_sql(table, schema=database, con=self.get_engine(), if_exists='append', index=False)
        except BaseException as e:
            raise ImportError(f'to_sql:【{database}.{table}】 {repr(e)}')

    def list_tables(self, database) -> list[str]:
        """
        列出指定数据库下所有的用户表
        """
        with self.get_engine().connect() as conn:
            conn.execute(text(f'use `{database}`'))
            rs = conn.execute(text(f'show tables;'))
            tables = list(map(lambda x: x[0], rs))
            return tables

    def list_databases(self) -> list[str]:
        """
        列出指定连接的所有数据库
        """
        with self.default_engine.connect() as conn:
            rs = conn.execute(text(f'show databases;'))
            databases = list(map(lambda x: x[0], rs))
            return databases

    def list_tables_without_index(self, database) -> list[str]:
        """
        列出指定数据库下，没有索引的表
        """
        with self.get_engine().connect() as conn:
            conn.execute(text(f'use `{database}`;'))
            rs = conn.execute(text(f'show tables;'))
            tables = list(map(lambda x: x[0], rs))
            sql = f"""SELECT
                            table_schema,
                            table_name,
                            count(DISTINCT index_name)
                        FROM
                            information_schema.STATISTICS 
                        WHERE
                            TABLE_SCHEMA = '{database}' 
                            and index_name != 'PRIMARY'
                        GROUP BY
                            table_schema,
                            table_name;
                    """
            table_with_indexs = conn.execute(text(sql))
            index_tables = list(map(lambda x: x[1], table_with_indexs))
            diff = set(tables).difference(set(index_tables))
            return diff

    def get_index_drop_sql(self, database) -> list[str]:
        """
        获取数据库中用户所有表的索引删除sql
        """
        drop_sqls = []
        with self.get_engine().connect() as conn:
            conn.execute(text(f'use `{database}`;'))
            rs = conn.execute(text(f'show tables;'))
            for r in rs:
                tablename = r[0]
                index_drop_sql = f"""SELECT
                                        TABLE_NAME,
                                        CONCAT(
                                            'ALTER TABLE `',
                                            TABLE_NAME,
                                            '` ',
                                            'DROP INDEX ',
                                            '`',
                                            INDEX_NAME,
                                            '`',
                                            ';' 
                                    ) AS 'Show_Drop_Indexes' 
                                    FROM
                                        information_schema.STATISTICS 
                                    WHERE
                                        TABLE_SCHEMA = '{database}' 
                                        AND TABLE_NAME = '{tablename}' 
                                        AND index_name != 'PRIMARY' 
                                    GROUP BY
                                        TABLE_NAME,
                                        INDEX_NAME 
                                    ORDER BY
                                        TABLE_NAME ASC,
                                        INDEX_NAME ASC;"""
                rows = conn.execute(text(index_drop_sql))
                for row in rows:
                    drop_sqls.append(row[1])
        return drop_sqls

    def get_index_alert_sqls(self, database) -> list[str]:
        index_alert_sqls = []
        with self.get_engine().connect() as conn:
            conn.execute(text(f'use `{database}`;'))
            rs = conn.execute(text(f'show tables;'))
            for r in rs:
                tablename = r[0]
                index_alert_sql = f"""SELECT
                            TABLE_NAME,
                             CONCAT(
                                    'ALTER TABLE `',
                                    '{database}`.`',
                                    TABLE_NAME,
                                    '` ',
                                    'ADD ',
                                IF
                                    (
                                        NON_UNIQUE = 1,
                                    CASE
                                            UPPER( INDEX_TYPE ) 
                                            WHEN 'FULLTEXT' THEN
                                            'FULLTEXT INDEX' 
                                            WHEN 'SPATIAL' THEN
                                            'SPATIAL INDEX' ELSE CONCAT( 'INDEX `', INDEX_NAME, '` USING ', INDEX_TYPE ) 
                                        END,
                                    IF
                                        (
                                            UPPER( INDEX_NAME ) = 'PRIMARY',
                                            CONCAT( 'PRIMARY KEY USING ', INDEX_TYPE ),
                                            CONCAT( 'UNIQUE INDEX `', INDEX_NAME, '` USING ', INDEX_TYPE ) 
                                        ) 
                                    ),
                                    '(',
                                    GROUP_CONCAT(
                                        DISTINCT CONCAT( '`', COLUMN_NAME, '`' ) 
                                    ORDER BY
                                        SEQ_IN_INDEX ASC SEPARATOR ', ' 
                                    ),
                                    ');' 
                            ) AS 'Show_Add_Indexes' 
                            FROM
                                information_schema.STATISTICS 
                            WHERE
                                TABLE_SCHEMA = '{database}' 
                                AND TABLE_NAME = '{tablename}' 
                                and index_name != 'PRIMARY'
                            GROUP BY
                                TABLE_NAME,
                                INDEX_NAME 
                            ORDER BY
                                TABLE_NAME ASC,
                                INDEX_NAME ASC;"""
                rows = conn.execute(text(index_alert_sql))
                for row in rows:
                    index_alert_sqls.append(row[1])
        return index_alert_sqls

    def get_table_index_alert_sqls(self, database, tablename) -> list[str]:
        """
        获取指定表的所有索引创建语句
        如果执行报错参考执行该语句：
        SET GLOBAL sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''));
        放开group by 的限制条件
        """
        index_alert_sqls = []
        with self.get_engine().connect() as conn:
            index_alert_sql = f"""SELECT
                                        TABLE_NAME,
                                         CONCAT(
                                                'ALTER TABLE `',
                                                '{database}`.`',
                                                TABLE_NAME,
                                                '` ',
                                                'ADD ',
                                            IF
                                                (
                                                    NON_UNIQUE = 1,
                                                CASE
                                                        UPPER( INDEX_TYPE ) 
                                                        WHEN 'FULLTEXT' THEN
                                                        'FULLTEXT INDEX' 
                                                        WHEN 'SPATIAL' THEN
                                                        'SPATIAL INDEX' ELSE CONCAT( 'INDEX `', INDEX_NAME, '` USING ', INDEX_TYPE ) 
                                                    END,
                                                IF
                                                    (
                                                        UPPER( INDEX_NAME ) = 'PRIMARY',
                                                        CONCAT( 'PRIMARY KEY USING ', INDEX_TYPE ),
                                                        CONCAT( 'UNIQUE INDEX `', INDEX_NAME, '` USING ', INDEX_TYPE ) 
                                                    ) 
                                                ),
                                                '(',
                                                GROUP_CONCAT(
                                                    DISTINCT CONCAT( '`', COLUMN_NAME, '`' ) 
                                                ORDER BY
                                                    SEQ_IN_INDEX ASC SEPARATOR ', ' 
                                                ),
                                                ') , ALGORITHM=INPLACE,LOCK=NONE;' 
                                        ) AS 'Show_Add_Indexes' 
                                        FROM
                                            information_schema.STATISTICS 
                                        WHERE
                                            TABLE_SCHEMA = '{database}' 
                                            AND TABLE_NAME = '{tablename}' 
                                            and index_name != 'PRIMARY'
                                        GROUP BY
                                            TABLE_NAME,
                                            INDEX_NAME 
                                        ORDER BY
                                            TABLE_NAME ASC,
                                            INDEX_NAME ASC;"""
            rows = conn.execute(text(index_alert_sql))
            for row in rows:
                index_alert_sqls.append(row[1])
        return index_alert_sqls

    def get_table_index_drop_sql(self, database, tablename) -> list[str]:
        """
        获取数据库中用户所有表的索引删除sql
        """
        drop_sqls = []
        with self.get_engine().connect() as conn:
            index_drop_sql = f"""SELECT
                                        TABLE_NAME,
                                        CONCAT(
                                            'ALTER TABLE `',
                                            '{database}`.`',
                                            TABLE_NAME,
                                            '` ',
                                            'DROP INDEX ',
                                            '`',
                                            INDEX_NAME,
                                            '`',
                                            ';' 
                                    ) AS 'Show_Drop_Indexes' 
                                    FROM
                                        information_schema.STATISTICS 
                                    WHERE
                                        TABLE_SCHEMA = '{database}' 
                                        AND TABLE_NAME = '{tablename}' 
                                        AND index_name != 'PRIMARY' 
                                    GROUP BY
                                        TABLE_NAME,
                                        INDEX_NAME 
                                    ORDER BY
                                        TABLE_NAME ASC,
                                        INDEX_NAME ASC;"""
            rows = conn.execute(text(index_drop_sql))
            for row in rows:
                drop_sqls.append(row[1])
        return drop_sqls

    def copy_data_to_new_table(self, source_database, source_table, condition, target_database, target_table):
        # 复制表结构
        create_sql = f"CREATE TABLE `{target_database}`.`{target_table}` (LIKE `{source_database}`.`{source_table}`)"
        print(f'复制表结构: {create_sql}')
        self.execute_update(create_sql)
        # 记录新表的索引
        index_alert_sqls = self.get_table_index_alert_sqls(target_database, target_table)
        # 删除新表的索引
        index_drop_sqls = self.get_table_index_drop_sql(target_database, target_table)
        for drop_sql in index_drop_sqls:
            print(f'删除新表的索引: {drop_sql}')
            self.execute_update(drop_sql)
        # 迁移数据到新表
        if not condition:
            condition = '1=1'
        insert_sql = f"INSERT IGNORE INTO `{target_database}`.`{target_table}` SELECT * FROM `{source_database}`.`{source_table}` where {condition}"
        print(f'迁移数据到新表: {insert_sql}')
        self.execute_update(insert_sql)
        # 恢复新表的索引
        for alert_sql in index_alert_sqls:
            print(f'恢复新表的索引: {alert_sql}')
            self.execute_update(alert_sql)

    def get_character_change_utf8mb4_sql(self, database) -> list[str]:
        """
        获取数据库中用户所有表的修改成utf8mb4的sql
        """
        alert_sqls = []
        get_alert_sql = f"""SELECT
                            CONCAT( "ALTER TABLE `",TABLE_SCHEMA,"`.`", TABLE_NAME, "` CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;" ) AS target_tables 
                        FROM
                            INFORMATION_SCHEMA.TABLES 
                        WHERE
                            TABLE_TYPE = "BASE TABLE" and TABLE_SCHEMA = '{database}';"""
        with self.get_engine().connect() as conn:
            rs = conn.execute(text(get_alert_sql))
            for r in rs:
                alert_sqls.append(r[0])
        return alert_sqls

    def get_table_create_sql(self, table, database=None) -> str:
        """
        获取表的创建语句
        :param table: 表名
        :param database: 数据库名
        :return:
        """
        if database:
            show_create_table_sql = f'show create table `{database}`.`{table}`'
        else:
            show_create_table_sql = f'show create table {table}'
        with self.get_engine().connect() as conn:
            rs = conn.execute(text(show_create_table_sql))
            return rs.one()[1]
        return None

    def get_database_create_sql(self, database=None) -> str:
        """
        获取数据库的创建语句
        :param database: 数据库名
        :return:
        """
        show_create_database_sql = f'show create database {database}'
        with self.get_engine().connect() as conn:
            rs = conn.execute(text(show_create_database_sql))
            return rs.one()[1]
        return None

    def get_table_columns(self, database, table):
        """
        获取表的字段列表
        :param database: 数据库名
        :param table: 表名
        :return:
        """
        show_column_sql = f"select * from information_schema.COLUMNS where table_name = '{table}' and table_schema = '{database}'"
        result = self.execute_query(show_column_sql)
        return result.fetchall()

    def exists_table_column(self, database, table, column):
        """
        判断表是否存在某个字段
        :param database: 数据库名
        :param table: 表名
        :param column: 字段名
        :return: True：存在   False：不存在
        """
        show_column_sql = f"select count(0) from information_schema.COLUMNS where table_name = '{table}' and table_schema = '{database}' and column_name = '{column}'"
        count = self.execute_query(show_column_sql).scalar()
        return True if count and count > 0 else False

    def exists_table(self, database, table):
        """
        是否存在指定表
        :param database: 数据库
        :param table: 表名
        :return: True:存在 False:不存在
        """
        with self.get_engine().connect() as conn:
            has_table = self.get_engine().dialect.has_table(conn, f"{table}", schema=database)
            return has_table

    def exists_database(self, database):
        """
        是否存在指定库
        :param database: 数据库
        :return: True:存在 False:不存在
        """
        with self.get_engine().connect() as conn:
            has_database = self.get_engine().dialect.has_schema(conn, database)
            return has_database

    def import_sql_file(self, sql_file, database):
        """
        导入sql文件
        :param sql_file:
        :param database:
        :return:
        """
        logger.info(f'【{database}】导入sql文件 {sql_file}')
        import_shell = f'{mysql_file} -v --host={self.host} --user={self.user} --password={self.password} --port={self.port} --max_allowed_packet=67108864 --net_buffer_length=16384 -D {database}< {sql_file}'
        execute_command(import_shell)
        pass
