import pandas as pd
from sqlalchemy import create_engine

from json import load

def CONNECT_TO_DB(database, soure_dir):
    with open(soure_dir + '/PACKAGE/DATALAKE/cred.json') as config_file:
        cred = load(config_file)

    # CREATE CONNECTION TO DATABASE
    engine = create_engine('mssql+pyodbc://'+ cred['username'] + ':'+ cred['password'] +'@'+ cred['server'] +'/'+ database +'?driver=ODBC+Driver+17+for+SQL+Server')
    return engine

def LOG(soure_dir, status, message):
    
    database = 'INT_DATALAKE_GOLD'

    with open(soure_dir + '/PACKAGE/DATALAKE/cred.json') as config_file:
        cred = load(config_file)

    # CREATE CONNECTION TO DATABASE
    engine = create_engine('mssql+pyodbc://'+ cred['username'] + ':'+ cred['password'] +'@'+ cred['server'] +'/'+ database +'?driver=ODBC+Driver+17+for+SQL+Server')

    #INSERT LOG
    log = pd.DataFrame({'PROCESS': 'INT_ETL_SILVER', 'DATE': pd.Timestamp.now(), 'STATUS': status, 'MESSAGE': message}, index=[0])
    log.to_sql('LOG', engine, if_exists='append')


def TL_TO_SILVER(**kwargs):
    # GET PARAMETERS
    database_source = kwargs['layer_source']
    table_source = kwargs['table_source']
    database = kwargs['layer']
    table = kwargs['table']
    source_dir = kwargs['source_dir']

    try:

        engine = CONNECT_TO_DB(database, source_dir)
        engine_source = CONNECT_TO_DB(database_source, source_dir)

        #LOAD DATA FROM SILVER LAYER
        chunk_size = 50000

        for chunk in pd.read_sql_table(table_source, engine_source, chunksize = chunk_size):
            chunk.to_sql(table, engine, if_exists='append')


        LOG(source_dir, 'SUCCESS', 'TL_TO_SILVER: ' + table + ' was successfully loaded into SILVER from BRONZE')
    
    except Exception as e:
        LOG(source_dir, 'ERROR', 'FAILED - ' + str(e))

    

    