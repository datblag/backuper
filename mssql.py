import os
import pymssql
import logging


# prm_sql_server {'name': server_name, 'user': user_login, 'password': user_password}
# prm_database [{'base_name': '', 'backup_name': '', 'server_path': '', 'net_path': ''}]
def archive_sql_database(prm_sql_server=None, prm_database=None, prm_is_only_differences=0):
    prm_sql_server = prm_sql_server if prm_sql_server else {}
    prm_database = prm_database if prm_database else []
    try:
        for cur_database in prm_database:
            try:
                logging.warning('Start '+cur_database['base_name']+' backup')
                backup_file_name = cur_database['backup_name'] + '.bak'
                str_diff = ''
                if prm_is_only_differences == 1:
                    str_diff = ',     DIFFERENTIAL'
                else:
                    if os.path.isfile(os.path.join(cur_database['net_path'], backup_file_name)):
                        os.remove(os.path.join(cur_database['net_path'], backup_file_name))
                conn = pymssql.connect(server=prm_sql_server['name'], user=prm_sql_server['user'],
                                       password=prm_sql_server['password'], database='', autocommit=True)

                conn.cursor().execute('''BACKUP DATABASE '''+cur_database['base_name']+''' to DISK=N'''+"'" + cur_database['server_path'] + backup_file_name + "'" + ''' WITH NOFORMAT
                                      ,NOINIT
                                      ,NAME = N'''+"'" + cur_database['backup_name'] + "'" + '''
                                      ,SKIP
                                      ,REWIND'''+str_diff+'''
                                      ,NOUNLOAD
                                      ,STATS = 10
                                      ,COMPRESSION''')
                logging.warning('Complete '+cur_database['base_name']+' backup')
            except Exception as e:
                logging.error(e)
    except Exception as e:
        logging.error(e)
