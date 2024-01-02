from usage import Rds01, Rds02
from utils import logger, config, str2bool


def export_csv(ent_code: str):
    """
    导出
    :param ent_code:
    :return:
    """
    rds_list = [Rds01(), Rds02()]
    for rds in rds_list:
        logger.info(f'【导出csv {rds.get_name()}】。。。')
        rds.export_parallel(ent_code)
    pass


def import_db(is_truncate_data: bool = False):
    """
    导入
    :param is_truncate_data:
    :return:
    """
    rds01 = Rds01()
    rds02 = Rds02()
    rds_list = [rds01, rds02]
    for rds in rds_list:
        logger.info(f'【开始导入csv {rds.get_name()}】。。。')
        rds.import_parallel(is_truncate_data)


if __name__ == '__main__':
    # 指定账套
    ent_code = config.get('global', 'ent_code')
    export_csv(ent_code)

    # 是否清空目标表数据
    is_truncate_data = str2bool(config.get('global', 'is_truncate_data'))
    import_db(is_truncate_data)
