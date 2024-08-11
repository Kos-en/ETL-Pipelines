 # LEARNING HOW TO BUILD SCALABLE AND EFFICIENT ETL PIPELINES WITH PYTHON AND AIRFLOW 

 ## OVERVIEW.
My facination of data inspired me to start my journey into understanding the nitty-gritty of how companies use etl pipelines to improve their business operations through insightful analysis and machine learning.In this repository/project I will be showing my progress in learning Extraction ,Transformating and Loading of data. I'll document each data pipeline process individually and at the end combine all what iv learnt to build a scalable ,secure and efficient ETL pipeline using Python and AWS.

### Extraction of data.
This section will focus mainly on extracting data from different sources.eg( API,Databases,csv and parquet files).
An example may be extracting data from postgresql using SQLAlchemy and psycopg2 python libraries
```python
#extracting data from postgresql
def source_data_from_table(db_name, user, password, host, port, table_name):
    try:
        # Create a database URL for SQLAlchemy
        db_url = f'postgresql://{user}:{password}@{host}:{port}/{db_name}'
        
        # Create an SQLAlchemy engine
        engine = create_engine(db_url)
        
        # Read SQL query results into a pandas DataFrame
        df_table = pd.read_sql(f"SELECT * FROM {table_name}", engine)
    except Exception as e:
        print(f"An error occurred: {e}")
        df_table = pd.DataFrame()
    
    return df_table
```
#### Results
View the rest of my notebook with other different ways of extracting data from data sources and grouping them into one using the function extracted_data() to form a simple extraction pipeline. [Extract.ipynb](Extract.ipynb)

### Transformation of data.
Transformation process in a pipeline is arguably the most difficult and rewarding since you get to see ur data come to life before loading it into data target.
In this section I learnt how to transform data using python and its library pandas.I also learnt logging in python where I used a third party logger called loguru to track errors in pipeline.
During the process of making the pipeline I first learnt how to build a transformation pipeline that uses only one function.I also learnt how to separate this function into different smaller functions that helped create an easy to understand pipeline and one that I could easily find errors and scale.
A snippet of the code used:
```python

#function for loading data
def loading_data(crash_data):
     df=pd.read_csv(crash_data)
     return df

#loading pipeline
def data_pipeline(crash_file,vehicle_crash_file):
    df_crash=pd.DataFrame()
    df_vehicle=pd.DataFrame()
    try:
        df_crash=loading_data(crash_file)
        df_vehicle=loading_data(vehicle_crash_file)
    except Exception as e:
        logger.info(f'Exception in reading data file:{e}')
    finally:
        return df_crash,df_vehicle 

 #function for merging crashes and vehicle_crashes data
def merging_tables(df_crashes,df_vehicles):
    df = df_crashes.merge(df_vehicles, how='left', on='crash_record_id', suffixes=('_left','_right')) 
    df = df.groupby('vehicle_type').agg({'crash_record_id': 'count'}).reset_index() 
    return df

    #merging pipeline
def merge_pipeline(df_crash,df_vehicle):
    try:
        df_merged=merging_tables(df_crash,df_vehicle)
    except Exception as e:
        logger.info('Exception in merging tables{e}') 
    finally:
        return df_merged 

```
Separating each function like this helps with modularity of code.
The rest of the code can be found here ->[transform.ipynb](python/transform.ipynb)

### Loading data
This is the final step in an etl pipeline. Here the data is loaded into a data target eg data warehouse where the data is stored.This stored data is normally used for data analytics and even Machine learning thus showing the importance of have accurate,clean and consistent data. Below is a simple loading process into a Relational database management system(Postgresql)
Here I created a SCHEMA and table using simple sql queries.
```SQL

CREATE SCHEMA chicago_dmv;

CREATE TABLE chicago_dmv.Vehicle
(             
    crash_unit_id INT PRIMARY KEY,
    crash_record_id TEXT,
    rd_no TEXT,
    crash_date TIMESTAMP,
    unit_no INT,
    unit_type VARCHAR(255),
    vehicle_id INT,
    make  TEXT,
    model  TEXT,
    lic_plate_state VARCHAR(255),
    vehicle_defect  VARCHAR(255),
    maneuver  VARCHAR(255),
    occupant_cnt  FLOAT   
);

CREATE TABLE chicago_dmv.Crash
(
    crash_record_id TEXT,
    crash_date TIMESTAMP,
    posted_speed_limit INT,
    traffic_control_device VARCHAR(255),
    weather_condition VARCHAR(255),
    lighting_condition VARCHAR(255),
    crash_hour INT,
    crash_day_of_week INT,
    crash_month INTEGER,
    latitude FLOAT,
    longitude FLOAT
);
ALTER TABLE chicago_dmv.Vehicle
ALTER COLUMN vehicle_id TYPE FLOAT 

ALTER TABLE chicago_dmv.Crash
ADD PRIMARY KEY (crash_record_id);

SELECT * FROM chicago_dmv.Crash
SELECT * FROM chicago_dmv.vehicle
```
Here I used a loop to iterate through the index and row of the dataframe df_new_vehicle_crashes.It iterates through each index and stores each value of each column per individual row to the tuple values_vehicle which then  loads them to postgresql using the cursor.

```Python
#loading data to postgres
for index , row in df_new_vehicle_crashes.iterrows():
    values_vehicle = (
            row['crash_unit_id'],
            row['crash_record_id'],
            row['rd_no'],
            row['crash_date'],
            row['unit_no'],
            row['unit_type'],
            row['vehicle_id'],
            row['make'],
            row['model'],
            row['lic_plate_state'],
            row['vehicle_defect'],
            row['maneuver'],
            row['occupant_cnt']
    )
    cur.execute(insert_query_vehicle, values_vehicle) 
```
The rest of the jupyter notebook can be found here ->[load.ipynb](python/load.ipynb)


# ETL Pipeline Project

## Overview

This project demonstrates a simple ETL (Extract, Transform, Load) pipeline using Docker, Apache Airflow, and Python. The pipeline extracts data, transforms it, and loads it into a PostgreSQL database.

## Project Structure

- **`dags/first_dag.py`**: Contains the main Airflow DAG that orchestrates the ETL process.
- **`.env`**: Environment variables for configuring services.
- **`docker-compose.yaml`**: Defines the Docker services for Apache Airflow and PostgreSQL.
- **`Dockerfile`**: Custom Docker image setup for the project.
- **`logs/`**: Directory containing logs generated by Airflow.

## Features

- **Dockerized Environment**: Uses Docker to containerize the application.

- **Airflow DAG**: Manages and schedules the ETL tasks. The DAG is defined in `dags/first_dag.py`.

- **PostgreSQL Database**: Stores the processed data.
- **Pgadmin**: Used to view the stored data.

## Setup and Installation

```bash
1. Clone the Repository
# This command will create a local copy of the repository on your machine.
https://github.com/Kos-en/ETL-Pipelines.git

# Change to the project directory
cd ETL-Pipelines

2.Ensure you have the necessary files to run airflow and docker.
- **docker-compose.yaml**
- **Dockerfile**
- **.env**
- **dags/first_dag.py**
- **.data**
- **.logs**

3. Build and Start Docker Containers
# Build the Docker images and start the services defined in docker-compose.yaml.
docker compose up airflow-init
docker-compose up --build

4. Access Apache Airflow
Open your web browser and go to http://localhost:8080 to access the Apache Airflow web interface.

Use the default credentials (verify in docker-compose.yaml if different):
Username: airflow
Password: airflow

Here you will also create a new connection in postgres with the details:
connection Id:postgres
connection type:Postgres
Host:postgres
Login:airflow
password:airflow(#confirm in ur yaml file setup)
port:5432

5. Monitor and Manage ETL Tasks

Use the Airflow web interface to monitor the status of your ETL tasks, view logs, and manage the DAG.

6. Database Access
You can view the data loaded in pgadmin by going to http://localhost:5050 this docker image is also created in the yaml file

7. View Logs
Logs generated by Airflow are stored in the logs/ directory.
Check these logs to monitor the ETL process and troubleshoot any issues.
