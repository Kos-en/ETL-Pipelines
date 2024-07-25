 # LEARNING HOW TO BUILD SCALABLE AND EFFICIENT ETL PIPELINES WITH PYTHON AND AWS. 

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
