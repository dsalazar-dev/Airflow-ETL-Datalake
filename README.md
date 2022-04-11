# AIRFLOW-ETL-DATALAKE

## SOFTWARE REQUERIED

- Apache Airflow
- Microsoft SQL Server 19
- Python 3.8

## INSTALL DATABASE

- Install Microsoft SQL Server 19
- Create three empty DB
  - INT_DATALAKE_BRONZE
  - INT_DATALAKE_SILVER
  - INT_DATALAKE_GOLD

## INSTALL AIRFLOW (ETL)

- Install Apache Airflow locally (Tutorial: https://youtu.be/aTaytcxy2Ck)
- Copy the resources from Airflow-Dags to directory of Dags of your Apache Airflow
- Start Docker and run Apache Airflow
- Create a cred.json file on PACKAGE\DATALAKE\ with the credentials of the DB 
  Ex. 
    {
        "server": "localhost:1433",
        "username": "sa",
        "password": "password"
    }
- Search and run the pipeline DAG_ETL_DATALAKE_001


## INTALL FLASK APP

- Install Python 3.8
- Create a virtual Env
- Copy the resources from Flask-APIM to local directory
- Create a cred.json file on the root directory with the credentials of the DB 
  Ex. 
    {
        "server": "localhost:1433",
        "username": "sa",
        "password": "password"
    }
- Execute the virtual env
- Run pip install requeriments.txt
- Exectue the Flask app with "py main.py"
