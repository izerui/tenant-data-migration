from base import *

# 调试模式下的表名，正式用应该设置为None，指定的话则只处理当前表
# debug_table = 'form_template_detail'
debug_table = None


class Rds02(BaseExport, BaseImport, BaseSync):

    def __init__(self):

        # rds02 mysql连接
        self.rds_host = config.get('source_mysql', 'rds_02_host')
        self.rds_port = config.get('source_mysql', 'rds_02_port')
        self.rds_user = config.get('source_mysql', 'rds_02_user')
        self.rds_pass = config.get('source_mysql', 'rds_02_pass')

        # rds02 mysql连接
        source_rds02 = Mysql(self.rds_host, self.rds_port, self.rds_user, self.rds_pass)

        # rds02 要导入的库
        databases_rds02 = ['manufacture', 'storehouse', 'qc']

        # 要导入的目的mysql
        self.target_host = config.get('target_mysql', 'host')
        self.target_port = config.get('target_mysql', 'port')
        self.target_user = config.get('target_mysql', 'user')
        self.target_pass = config.get('target_mysql', 'pass')
        target = Mysql(self.target_host, self.target_port, self.target_user, self.target_pass)

        # 初始化导出对象
        BaseExport.__init__(self, source_rds02,
                            databases_rds02,
                            dumps_folder,
                            max_workers=6)
        # 初始化导入对象
        BaseImport.__init__(self, source_rds02, target, databases_rds02, dumps_folder)

        # 初始化同步对象(独立同步逻辑，与上面的导出导入无关)
        BaseSync.__init__(self, source_rds02, target, databases_rds02)

    def get_name(self):
        return 'rds02'

    def single_table_for_debug(self):
        return debug_table

    def chunk_wrapper(self, df, database, table, is_sync=False):
        def is_db_tbl(db, tbl):
            return database == db and table == tbl

        if 'id' in df.columns:
            df['id'] = None
        if is_sync:
            return df
        else:
            if is_db_tbl('manufacture', 'customer'):
                # 过滤掉name为空的数据
                # df = df[df['name'].astype(str).str.strip() != '']

                # 将字段name为空的补充为''
                df['name'] = df['name'].fillna('')
                pass
            return df

    def table_data_match_filter(self, database, table):
        def is_db_tbl(db, tbl):
            return database == db and table == tbl

        if '_bakup_' in table or '_20231203' in table or '_0601' in table or '_backups' in table or '_copy1' in table:
            return False
        return super().table_data_match_filter(database, table)