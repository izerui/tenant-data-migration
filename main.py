from base import logger
from rds01 import Rds01
from rds02 import Rds02

if __name__ == '__main__':
    # 指定账套
    ent_codes = ['638334323', '736482969', '333367878', 'cefeeed1-d198-4214-b1a2-9277e9f78655', '30edefb6-51af-447d-82cd-07cff070e2a2', '4f1ec7fd-74c8-4105-85c0-2cdd44307374']

    # 测试模式只同步前10条记录
    test_data = False
    # 是否删除原来的租户数据或者平台数据
    delete_data = False
    # 是否同步前删除数据库
    drop_database = False
    # 是否同步平台表数据
    sync_platform_data = False
    # 是否同步租户数据
    sync_tenant_data = True

    rds01 = Rds01(databases=['cloud_sale', 'crm', 'customer_supply', 'data_authority', 'development', 'billing',
                             'dictionary', 'form_template', 'freeze', 'hr', 'hrmis', 'mrp', 'price_center', 'cloud_finance',
                             'printer_center', 'purchase', 'rbac_new', 'supplier', 'system_setting', 'ufile_store',
                             'unicom', 'wx_applet'])
    rds02 = Rds02(databases=['manufacture', 'storehouse', 'qc'])
    rds_list = [rds01, rds02]
    for rds in rds_list:
        logger.info(f'【开始同步 {rds.get_name()}】准备数据。。。')
        rds.sync_parallel(ent_codes,
                          test_data=test_data,
                          delete_data=delete_data,
                          drop_database=drop_database,
                          sync_platform_data=sync_platform_data,
                          sync_tenant_data=sync_tenant_data)
