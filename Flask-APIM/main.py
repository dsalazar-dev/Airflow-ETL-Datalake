from flask import Flask
from flask import Response

from sqlalchemy import create_engine
from json import load
import pandas as pd

def CONNECT_TO_DB(database):
    with open('cred.json') as config_file:
        cred = load(config_file)

    # CREATE CONNECTION TO DATABASE
    engine = create_engine('mssql+pyodbc://'+ cred['username'] + ':'+ cred['password'] +'@'+ cred['server'] +'/'+ database +'?driver=ODBC+Driver+17+for+SQL+Server')
    return engine

app = Flask(__name__)

@app.route('/gold/m2', methods=['GET'])
def GOLD_M2():
    engine = CONNECT_TO_DB('INT_DATALAKE_GOLD')
    df = pd.read_sql_query("EXEC PRC_GET_M2", engine)
    return Response(df.to_json(orient="records"), mimetype='application/json')

@app.route('/gold/m3', methods=['GET'])
def GOLD_M3():
    engine = CONNECT_TO_DB('INT_DATALAKE_GOLD')
    df = pd.read_sql_query("EXEC PRC_GET_M3", engine)
    return Response(df.to_json(orient="records"), mimetype='application/json')

@app.route('/log', methods=['GET'])
def LOG():
    engine = CONNECT_TO_DB('INT_DATALAKE_GOLD')
    df = pd.read_sql_query("EXEC PRC_GET_LOGS", engine)
    return Response(df.to_json(orient="records"), mimetype='application/json')

@app.route('/sql1', methods=['GET'])
def SQL1():
    engine = CONNECT_TO_DB('INT_DATALAKE_SILVER')
    df = pd.read_sql_query("EXEC PRC_GET_SQL1", engine)
    return Response(df.to_json(orient="records"), mimetype='application/json')

@app.route('/sql2', methods=['GET'])
def SQL2():
    engine = CONNECT_TO_DB('INT_DATALAKE_SILVER')
    df = pd.read_sql_query("EXEC PRC_GET_SQL2", engine)
    return Response(df.to_json(orient="records"), mimetype='application/json')


if __name__ == '__main__':
   app.run()