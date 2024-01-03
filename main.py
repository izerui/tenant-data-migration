from base import logger, config
from usage import Rds01, Rds02

if __name__ == '__main__':
    # 指定账套
    ent_code = config.get('global', 'ent_code')
    # 测试模式只同步前10条记录
    test_data = False
    # 是否需要重建库和表结构
    create_structure = True
    # 是否同步平台表数据
    sync_platform_data = True
    # 是否同步租户数据
    sync_tenant_data = True
    rds01 = Rds01()
    rds02 = Rds02()
    rds_list = [rds01, rds02]
    for rds in rds_list:
        logger.info(f'【开始同步 {rds.get_name()}】准备数据。。。')
        rds.sync_parallel(ent_code, test_data=test_data, create_structure=create_structure,
                          sync_platform_data=sync_platform_data,
                          sync_tenant_data=sync_tenant_data)
