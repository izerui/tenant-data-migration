import logging
import os
import time
from configparser import ConfigParser
from subprocess import Popen, PIPE, STDOUT

import pandas as pd
from pandas import DataFrame
from tqdm import tqdm

__version__ = '2.0.9'
__all__ = [
    'log_time', 'logger', 'file_path', 'exe_command', 'BColors', 'str2bool', 'config'
]

LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(format=LOG_FORMAT, level=logging.INFO)
# 禁用 httpx 的日志输出
logging.getLogger('httpx').setLevel(logging.WARNING)
# 全局日志输出对象
logger = logging.getLogger()


class BColors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


def log_time(func):
    def wrapper(*args, **kwargs):
        # 在调用原始函数前添加新的功能，或在后面添加
        s_time = time.time()
        # 调用原始函数
        result = func(*args, **kwargs)
        # 在结果之前或结果之后添加其他内容
        e_time = time.time()
        logger.info(f'{repr(func)} 耗时: {e_time - s_time}秒')
        return result

    return wrapper


def file_path(path=''):
    """
    获取python运行根目录, 并且join传入的path  类似： python/data/..
    :return:
    """
    _path = os.path.dirname(__file__)
    if path:
        path_split = str(path).split('/')
        for split in path_split:
            _path = os.path.join(_path, split)
    return _path


def exe_command(command):
    """
    执行 shell 命令并实时打印输出
    :param command: shell 命令
    :return: process, exitcode
    """
    print(command)
    process = Popen(command, stdout=PIPE, stderr=STDOUT, shell=True)
    with process.stdout:
        for line in iter(process.stdout.readline, b''):
            try:
                print(line.decode().strip())
            except:
                print(str(line))
    exitcode = process.wait()
    if exitcode != 0:
        print('错误: 命令执行失败, 继续下一条... ')
    return process, exitcode


def iterator2dataframes(iterator, chunk_size: int, total_size: int) -> DataFrame:
    """Turn an iterator into multiple small pandas.DataFrame
    This is a balance between memory and efficiency
    """
    records = []
    frames = []
    pbar = tqdm(total=total_size)
    for i, record in enumerate(iterator):
        records.append(record)
        if i % chunk_size == chunk_size - 1:
            frames.append(pd.DataFrame(records))
            pbar.update(len(records))
            records = []
    if records:
        frames.append(pd.DataFrame(records))
    return pd.concat(frames)


def str2bool(v):
    return v.lower() in ("yes", "true", "t", "1")


config = ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), 'config.ini'))
