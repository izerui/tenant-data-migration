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
    for db in databases:
        tables = mysql.list_tables(database=db)
        for table in tables:
            is_tenant = mysql.exists_table_column(database=db, table=table, column='ent_code')
            if is_tenant:
                mysql.execute_update(f'truncate table {table}', database=db)
