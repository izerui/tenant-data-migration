# 租户镜像数据迁移

python -m venv venv

source ./venv/bin/activate

pip config set global.index-url https://mirror.baidu.com/pypi/simple/

* `pip install pymysql pandas SQLAlchemy tqdm`