import json

from base import BaseExport, BaseImport, BaseSync
from sink import Mysql
from utils import config

# 导出的csv临时目录
dumps_folder = config.get('global', 'dumps_folder')

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
        source_rds01 = Mysql(config.get('source_mysql', 'rds_01'))
        # rds01 要导入的表
        databases_rds01 = ['cloud_sale', 'crm', 'customer_supply', 'data_authority', 'development',
                           'dictionary', 'form_template', 'freeze', 'hr', 'hrmis', 'mrp', 'price_center',
                           'printer_center', 'purchase', 'rbac_new', 'supplier', 'system_setting', 'ufile_store',
                           'unicom', 'workflow', 'wx_applet']
        # 要导入的目的mysql
        target = Mysql(config.get('target_mysql', 'url'))

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

    def chunk_wrapper(self, df, database, table):
        """
        对csv读取的df对象进行二次处理，保证导入顺利
        :param df: df对象
        :param database: 要导入的数据库
        :param table: 要导入的表
        :return: 处理后的df对象
        """

        def is_db_tbl(db, tbl):
            return database == db and table == tbl

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
        if '_bakup_' in table or '_20231203' in table or '_0601' in table or '_backups' in table or '_copy1' in table or 'demand_result_finished' == table:
            return False
        return super().table_match_filter(database, table)


class Rds02(BaseExport, BaseImport, BaseSync):

    def __init__(self):
        # rds02 mysql连接
        source_rds02 = Mysql(config.get('source_mysql', 'rds_02'))
        # rds02 要导入的库
        databases_rds02 = ['manufacture', 'storehouse', 'qc']
        # 要导入的目的mysql
        target = Mysql(config.get('target_mysql', 'url'))

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

    def chunk_wrapper(self, df, database, table):
        def is_db_tbl(db, tbl):
            return database == db and table == tbl

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
