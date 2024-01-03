import json

from base import *

# 调试模式下的表名，正式用应该设置为None，指定的话则只处理当前表
# debug_table = 'form_template_detail'
debug_table = None


def format_json(text):
    if isinstance(text, float):
        return None
    else:
        text = str(text).replace('\'', '\"')
        text = str(text).replace(':True', ':true')
        text = str(text).replace(': True', ': true')
        text = str(text).replace(':False', ':false')
        text = str(text).replace(': False', ': false')
        return json.dumps(json.loads(text))


class Rds01(BaseExport, BaseImport, BaseSync):

    def __init__(self):

        # rds01 mysql连接
        self.rds_host = config.get('source_mysql', 'rds_01_host')
        self.rds_port = config.get('source_mysql', 'rds_01_port')
        self.rds_user = config.get('source_mysql', 'rds_01_user')
        self.rds_pass = config.get('source_mysql', 'rds_01_pass')

        source_rds01 = Mysql(self.rds_host, self.rds_port, self.rds_user, self.rds_pass)
        # rds01 要导入的表
        databases_rds01 = ['cloud_sale', 'crm', 'customer_supply', 'data_authority', 'development',
                           'dictionary', 'form_template', 'freeze', 'hr', 'hrmis', 'mrp', 'price_center',
                           'printer_center', 'purchase', 'rbac_new', 'supplier', 'system_setting', 'ufile_store',
                           'unicom', 'wx_applet']

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
        对csv读取的df对象进行二次处理，保证导入顺利
        :param df: df对象
        :param database: 要导入的数据库
        :param table: 要导入的表
        :param is_sync: 是否是同步数据模式(该模式下没有中间商，故部分数据不需要处理)
        :return: 处理后的df对象
        """

        def is_db_tbl(db, tbl):
            return database == db and table == tbl

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

    def table_match_filter(self, database, table):
        """
        匹配表过滤器，如果返回true则处理，否则跳过当前表继续下一个
        :param database: 数据库
        :param table: 表名
        :return: True:处理当前表  False:不处理当前表
        """
        if '_bakup_' in table or '_20231203' in table or '_0601' in table or '_backups' in table or '_copy1' in table or 'demand_result_finished' == table:
            return False
        # 标签打印模版
        if database == 'printer_center' and table == 'printer_template':
            return False
        if database == 'form_template' and table == 'element_config':
            return False
        if database == 'form_template' and table == 'form_template_detail':
            return False
        return super().table_match_filter(database, table)


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

    def table_match_filter(self, database, table):
        if '_bakup_' in table or '_20231203' in table or '_0601' in table or '_backups' in table or '_copy1' in table or 'demand_result_finished' == table:
            return False
        return super().table_match_filter(database, table)
