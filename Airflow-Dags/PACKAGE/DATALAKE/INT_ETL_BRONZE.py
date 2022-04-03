
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
    log = pd.DataFrame({'PROCESS': 'INT_ETL_BRONZE', 'DATE': pd.Timestamp.now(), 'STATUS': status, 'MESSAGE': message}, index=[0])
    log.to_sql('LOG', engine, if_exists='append')

    engine.dispose()
   


def EL_TO_BRONZE(**kwargs):

    # GET PARAMETERS
    database = kwargs['layer']
    table = kwargs['table']
    source_dir = kwargs['source_dir']
    file_path = kwargs['file_path']

    try:

        engine = CONNECT_TO_DB(database, source_dir)

        # FOR MASSIVE DATA WE LOAD WITH CHUNKSIZE
        chunk_size = 50000

        for chunk in pd.read_csv(file_path, chunksize = chunk_size):
            chunk.to_sql(table, engine, if_exists='append')
        
        engine.dispose()
        
        LOG(source_dir, 'SUCCESS', 'EL_TO_BRONZE: ' + table + ' was successfully loaded into BRONZE from SOURCE')
    
    except Exception as e:
        LOG(source_dir, 'ERROR', 'FAILED - ' + str(e))

