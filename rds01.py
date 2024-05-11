from base import *

# 调试模式下的表名，正式用应该设置为None，指定的话则只处理当前表
# debug_table = 'form_template_detail'
debug_table = None


class Rds01(BaseExport, BaseImport, BaseSync):

    def __init__(self, databases: list[str]):

        # rds01 配置
        self.rds_host = config.get('source_mysql', 'rds_01_host')
        self.rds_port = config.get('source_mysql', 'rds_01_port')
        self.rds_user = config.get('source_mysql', 'rds_01_user')
        self.rds_pass = config.get('source_mysql', 'rds_01_pass')

        # rds01 连接
        source_rds01 = Mysql(self.rds_host, self.rds_port, self.rds_user, self.rds_pass)

        # rds01 数据库
        databases_rds01 = databases

        # 要导入的目的mysql
        self.target_host = config.get('target_mysql', 'host')
        self.target_port = config.get('target_mysql', 'port')
        self.target_user = config.get('target_mysql', 'user')
        self.target_pass = config.get('target_mysql', 'pass')
        target = Mysql(self.target_host, self.target_port, self.target_user, self.target_pass)

        # 初始化导出对象
        BaseExport.__init__(self, source_rds01,
                            databases_rds01,
                            dumps_folder,
                            max_workers=8)
        # 初始化导入对象
        BaseImport.__init__(self, source_rds01, target, databases_rds01, dumps_folder)

        # 初始化同步对象(独立同步逻辑，与上面的导出导入无关)
        BaseSync.__init__(self, source_rds01, target, databases_rds01)

    def get_name(self):
        return 'rds01'

    def single_table_for_debug(self):
        return debug_table

    def return_before_handle_data(self, database, table):
        """
        前置处理器，不做索引删除和恢复
        :param database:
        :param table:
        :return:
        """
        if database == 'workflow':
            return None
        return super().return_before_handle_data(database, table)

    def get_columns_dtype(self, database, table):
        """
        根据库和表名返回指定的字段类型
        :param database:
        :param table:
        :return:
        """

        def is_db_tbl(db, tbl):
            return database == db and table == tbl

        # if is_db_tbl('form_template', 'element_config'):
        #     return {
        #         'element_describe': np.str_
        #     }
        return None

    def chunk_wrapper(self, df, database, table, is_sync=False):
        """
        导入数据前，进行的二次包装处理，比如过滤数据
        :param df: 读取的csv文件到pandas对象
        :param database: 要导入的数据库
        :param table: 要导入到的表
        :param is_sync: 是否是同步数据模式(该模式下没有中间商，故部分数据不需要处理)
        :return: pandas DataFrame 对象
        """

        def is_db_tbl(db, tbl):
            return database == db and table == tbl

        def is_db(db):
            return database == db

        # 除了ent表，其他表将id置为空，因为ent表的id在bom中用上了，可能bom重构后就不需要了
        if not table == 'ent':
            if 'id' in df.columns:
                df['id'] = None
        if is_sync:
            if is_db_tbl('rbac_new', 'ent'):
                df['name'] = df['name'].apply(lambda x: f'uat.{x}')
            return df
        else:
            if is_db_tbl('cloud_sale', 'balance_todo'):
                # 过滤掉customer_material_code字段是空字符串的数据
                # df = df[df['customer_material_code'].astype(str).str.strip() != '']

                # 将字段customer_material_code为空的补充为''
                df['customer_material_code'] = df['customer_material_code'].fillna('')
                df['customer_inventory_name'] = df['customer_inventory_name'].fillna('')
                df['customer_inventory_spec'] = df['customer_inventory_spec'].fillna('')
                df['sale_inventory_record_id'] = df['sale_inventory_record_id'].fillna('')
                df['inventory_id'] = df['inventory_id'].fillna('')
                df['inventory_name'] = df['inventory_name'].fillna('')
            if is_db_tbl('crm', 'customer'):
                # 过滤掉name为空的数据
                # df = df[df['name'].astype(str).str.strip() != '']

                # 将字段name为空的补充为''
                df['name'] = df['name'].fillna('')
                pass
            if is_db_tbl('unicom', 'purchase_coordination_file_type'):
                # 将creator为空的补充为''
                df['creator'] = df['creator'].fillna('')
            if is_db_tbl('form_template', 'element_config'):
                df['element_describe'] = df['element_describe'].apply(format_json)
            if is_db_tbl('form_template', 'form_template_detail'):
                df['template_json'] = df['template_json'].apply(format_json)
            if is_db_tbl('cloud_sale', 'sale_proposal'):
                df['customer_material_code'] = df['customer_material_code'].fillna('')
            return df

    def table_ddl_match_filter(self, database, table):
        """
        匹配表过滤器，如果返回true则处理表结构，否则跳过当前表继续下一个
        :param database: 数据库
        :param table: 表名
        :return: True:处理当前表结构  False:不处理当前表结构
        """

        def is_db_tbl(db, tbl):
            return database == db and table == tbl
        # 不同步spring batch 相关表
        if table.startswith('batch_job_') or table.startswith('batch_step_'):
            return False
        return super().table_ddl_match_filter(database, table)

    def table_data_match_filter(self, database, table):
        """
        匹配表过滤器，如果返回true则处理，否则跳过当前表继续下一个
        :param database: 数据库
        :param table: 表名
        :return: True:处理当前表  False:不处理当前表
        """

        def is_db_tbl(db, tbl):
            return database == db and table == tbl

        if table.endswith(
                '_bak') or '_bakup_' in table or '_20231203' in table or '_0601' in table or '_backups' in table or '_copy1' in table or 'demand_result_finished' == table:
            return False
        # 标签打印模版
        if is_db_tbl('printer_center', 'printer_template'):
            return False
        if is_db_tbl('form_template', 'element_config'):
            return False
        if is_db_tbl('form_template', 'form_template_detail'):
            return False
        if is_db_tbl('mrp', 'purge_demand'):
            return False
        if is_db_tbl('mrp', 'demand_log'):
            return False
        if is_db_tbl('development', 'bom_bak'):
            return False
        if 'batch_job' in table or 'batch_step' in table:
            return False
        if is_db_tbl('cloud_finance', 'pre_voucher_detail'):
            return False
        return super().table_data_match_filter(database, table)
