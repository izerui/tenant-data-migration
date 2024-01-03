from abc import ABCMeta, abstractmethod


class Interface(object):
    """
    导入导共有基础接口
    """
    __metaclass__ = ABCMeta  # 指定这是一个抽象类

    @abstractmethod
    def get_name(self):
        return None


class ExportInterface(Interface):
    """
    从rds导出数据接口
    """
    __metaclass__ = ABCMeta  # 指定这是一个抽象类

    def table_match_filter(self, database, table):
        """
        匹配表过滤器，如果返回true则处理，否则跳过当前表继续下一个
        :param database: 数据库
        :param table: 表名
        :return: True:处理当前表  False:不处理当前表
        """
        return True

    def single_table_for_debug(self):
        """
        返回单独某一个表，调试状态使用
        :return:
        """
        return None


class ImportInterface(Interface):
    """
    从外部导入数据到mysql接口
    """
    __metaclass__ = ABCMeta  # 指定这是一个抽象类

    def chunk_wrapper(self, df, database, table, is_sync=False):
        """
        导入数据前，进行的二次包装处理，比如过滤数据
        :param df: 读取的csv文件到pandas对象
        :param database: 要导入的数据库
        :param table: 要导入到的表
        :param is_sync: 是否是同步数据模式(该模式下没有中间商，故部分数据不需要处理)
        :return: pandas DataFrame 对象
        """
        return df
