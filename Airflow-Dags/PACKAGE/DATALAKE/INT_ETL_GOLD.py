import pandas as pd
from sqlalchemy import create_engine

from json import load
from itertools import combinations

def CONNECT_TO_DB(database, soure_dir):
    with open(soure_dir + '/PACKAGE/DATALAKE/cred.json') as config_file:
        cred = load(config_file)

    # CREATE CONNECTION TO DATABASE
    engine = create_engine('mssql+pyodbc://'+ cred['username'] + ':'+ cred['password'] +'@'+ cred['server'] +'/'+ database +'?driver=ODBC+Driver+17+for+SQL+Server')
    return engine

def LOG(engine, status, message):
    #INSERT LOG
    log = pd.DataFrame({'PROCESS': 'INT_ETL_GOLD', 'DATE': pd.Timestamp.now(), 'STATUS': status, 'MESSAGE': message}, index=[0])
    log.to_sql('LOG', engine, if_exists='append')

def TL_TO_GOLD(**kwargs):
    # GET PARAMETERS
    database_source = kwargs['layer_source']
    database = kwargs['layer']
    table = kwargs['table']
    source_dir = kwargs['source_dir']

    try:

        engine = CONNECT_TO_DB(database, source_dir)
        engine_source = CONNECT_TO_DB(database_source, source_dir)

        #M2. Trips with similar origin, destination, and time of day should be grouped together
        chunk_size = 50000

        for chunk in pd.read_sql_table(table, engine_source, chunksize = chunk_size):
            chunk["Trips"] = chunk.groupby(["origin_coord", "destination_coord", "datetime"]).grouper.group_info[0] + 1

            chunk.drop(["level_0", "index"], axis=1, inplace=True)	
            
            chunk.to_sql('M2', engine, if_exists='append')

            LOG(engine, 'SUCCESS', 'TL_TO_GOLD: ' + table + ' was successfully transformated M2 from SILVER to GOLD')


        #M3. Develop a way to obtain the weekly average number of trips for an area, defined by a bounding box (given by coordinates) or by a region
        
        #SELECT DATA FROM GOLD LAYER AND PARTITIONS BY REGION
        results = []
        regions = pd.read_sql_query("SELECT DISTINCT(region) FROM " + table, engine_source)

        for index, row in regions.iterrows():
            chunk_size = 50000

            print("SELECT * FROM " + table + " WHERE region = '" + row['region'] + "'")

            for chunk in pd.read_sql_query("SELECT * FROM " + table + " WHERE region = '" + row['region'] + "'", engine_source, chunksize = chunk_size):
                chunk.datetime = pd.to_datetime(chunk.datetime)
                chunk['WeekAvgNumber']=chunk.groupby('region').datetime.diff().dt.days.cumsum().fillna(0)//7
                chunk['WeekAvgNumber'] = chunk['WeekAvgNumber'].abs()
                chunk.groupby(['region','WeekAvgNumber']).agg({"region":"first"})
                chunk.drop(["level_0", "index"], axis=1, inplace=True)
                chunk.to_sql('M3', engine, if_exists='append')

       
        LOG(engine, 'SUCCESS', 'TL_TO_GOLD: ' + table + ' was successfully transformated M3 from SILVER to GOLD')
    
    except Exception as e:
        LOG(engine, 'ERROR', 'FAILED - ' + str(e))