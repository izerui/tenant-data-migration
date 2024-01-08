from base import logger, config
from rds01 import Rds01
from rds02 import Rds02

if __name__ == '__main__':
    # 指定账套
    # 鑫之都
    # ent_code='638334323'
    # 深圳安瑞创
    # ent_code='30edefb6-51af-447d-82cd-07cff070e2a2'
    # 东莞安瑞创
    # ent_code='4f1ec7fd-74c8-4105-85c0-2cdd44307374'
    # 靖邦
    ent_code = '1a92f2b4-08d3-4722-a733-eef69d55ecf4'

    # 测试模式只同步前10条记录
    test_data = False
    # 是否需要重建库和表结构
    create_structure = False
    # 是否同步平台表数据
    sync_platform_data = False
    # 是否同步租户数据
    sync_tenant_data = True
    rds01 = Rds01(databases=['cloud_sale', 'crm', 'customer_supply', 'data_authority', 'development',
                             'dictionary', 'form_template', 'freeze', 'hr', 'hrmis', 'mrp', 'price_center',
                             'printer_center', 'purchase', 'rbac_new', 'supplier', 'system_setting', 'ufile_store',
                             'unicom', 'wx_applet', 'test'])
    rds02 = Rds02(databases=['manufacture', 'storehouse', 'qc'])
    rds_list = [rds01, rds02]
    for rds in rds_list:
        logger.info(f'【开始同步 {rds.get_name()}】准备数据。。。')
        rds.sync_parallel(ent_code, test_data=test_data, create_structure=create_structure,
                          sync_platform_data=sync_platform_data,
                          sync_tenant_data=sync_tenant_data)
