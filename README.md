# AIRFLOW-ETL-DATALAKE

## SOFTWARE REQUIRED

- Apache Airflow
- Microsoft SQL Server 19
- Python 3.8

## INSTALL DATABASE

- Install Microsoft SQL Server 19
- Create three empty DB
  - INT_DATALAKE_BRONZE
  - INT_DATALAKE_SILVER
  - INT_DATALAKE_GOLD
- After the installation of Apache Airflow. In the database INT_DATALAKE_GOLD create the stored procedures located on the folder SQL

## INSTALL APACHE AIRFLOW (ETL)

- Install Apache Airflow locally (Tutorial: https://youtu.be/aTaytcxy2Ck)
- Start Docker and run Apache Airflow
- Copy the resources from Airflow-Dags to directory of Dags and refresh Apache Airflow
- Create a cred.json file on PACKAGE\DATALAKE\ with the credentials of the DB 
  Ex. 
    {
        "server": "localhost:1433",
        "username": "sa",
        "password": "password"
    }
- Search and run the pipeline DAG_ETL_DATALAKE_001


## INSTALL FLASK APP

- Install Python 3.8
- Create a virtual Env "py -m venv .venv"
- Copy the resources from Flask-APIM to local directory
- Create a cred.json file on the root directory with the credentials of the DB 
  Ex. 
    {
        "server": "localhost:1433",
        "username": "sa",
        "password": "password"
    }
- Execute the virtual env
- Run "pip install -r requeriments.txt"
- Exectue the Flask app with "py main.py"

## Connect PBI with Flask
- Open PBI Desktop (https://www.microsoft.com/en-us/download/details.aspx?id=58494)
- Check the source on File -> Options and Setings -> Data source settings
