from base import *

# 调试模式下的表名，正式用应该设置为None，指定的话则只处理当前表
# debug_table = 'form_template_detail'
debug_table = None


class Platform02(BaseExport, BaseImport, BaseSync):

    def __init__(self, databases: list[str]):

        # rds02 配置
        self.rds_host = config.get('rds02_mysql', 'host')
        self.rds_port = config.get('rds02_mysql', 'port')
        self.rds_user = config.get('rds02_mysql', 'user')
        self.rds_pass = config.get('rds02_mysql', 'pass')

        # rds02 连接
        source_rds02 = Mysql(self.rds_host, self.rds_port, self.rds_user, self.rds_pass)

        # rds02 数据库
        databases_rds02 = databases

        # 要导入的目的mysql
        self.target_host = config.get('platform_uat_mysql', 'host')
        self.target_port = config.get('platform_uat_mysql', 'port')
        self.target_user = config.get('platform_uat_mysql', 'user')
        self.target_pass = config.get('platform_uat_mysql', 'pass')
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

        # 除了ent表，其他表将id置为空，因为ent表的id在bom中用上了，可能bom重构后就不需要了
        if not table == 'ent':
            if 'id' in df.columns:
                df['id'] = None
        if is_db_tbl('platform_rbac', 'ent'):
            df['name'] = df['name'].apply(lambda x: f'uat.{x}')
        if is_db_tbl('platform_rbac', 'account'):
            df['password'] = '56b291d6ed9b9cb8e2d3dc09cb6377b9'
            df['salt'] = '123456'
        return df

    def table_data_match_filter(self, database, table):
        def is_db_tbl(db, tbl):
            return database == db and table == tbl

        if '_bakup_' in table or '_20231203' in table or '_0601' in table or '_copy1' in table:
            return False
        return super().table_data_match_filter(database, table)
