from tqdm import tqdm

from base import Mysql, config

if __name__ == '__main__':
    databases = ['cloud_sale', 'crm', 'customer_supply', 'data_authority', 'development', 'billing',
                 'dictionary', 'form_template', 'freeze', 'hr', 'hrmis', 'mrp', 'price_center', 'cloud_finance',
                 'printer_center', 'purchase', 'rbac_new', 'supplier', 'system_setting', 'ufile_store',
                 'unicom', 'wx_applet', 'manufacture', 'storehouse', 'qc']
    host = config.get('target_mysql', 'host')
    port = config.get('target_mysql', 'port')
    user = config.get('target_mysql', 'user')
    password = config.get('target_mysql', 'pass')
    mysql = Mysql(host, port, user, password)

    print('加入待处理列表:')
    list = []
    for db in databases:
        tables = mysql.list_tables(database=db)
        for table in tables:
            is_tenant = mysql.exists_table_column(database=db, table=table, column='ent_code')
            if is_tenant:
                db_table = f'`{db}`.`{table}`'
                list.append(db_table)
                print(db_table)


    bar = tqdm(total=len(list), desc='清除租户数据。。。')
    for db_table in list:
        mysql.execute_update(f'truncate table {db_table}')
        bar.update(1)
