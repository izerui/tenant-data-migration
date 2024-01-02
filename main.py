from usage import Rds01, Rds02
from utils import logger, config

if __name__ == '__main__':
    # 指定账套
    ent_code = config.get('global', 'ent_code')

    rds01 = Rds01()
    rds02 = Rds02()
    rds_list = [rds01, rds02]
    for rds in rds_list:
        logger.info(f'【开始同步 {rds.get_name()}】准备数据。。。')
        rds.sync_parallel(ent_code)
