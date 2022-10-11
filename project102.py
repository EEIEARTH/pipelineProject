from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.dates import days_ago
import pandas as pd

MYSQL_CONNECTION = 'mysql_default' # name of connection in airflow

#Path
salary_path = "gs://asia-southeast1-project102-2fe5b97d-bucket/data/salary.csv"
clean_path = "gs://asia-southeast1-project102-2fe5b97d-bucket/data/output102.csv"
##

def get_data_from_mysql(salary_path) : ## Get data

    # get path from task
    # use mysqlhook connect to MySQL DB
    mysqlserver = MySqlHook(MYSQL_CONNECTION)
    # Query data and 
    DS_Salary = mysqlserver.get_pandas_df(sql = "SELECT * FROM DS_Salary")
    # Drop column ที่ไม่ใช้
    DS_Salary = DS_Salary.drop('remote_ratio',axis =1)
    # Save file to CSV 
    # Save file to GCS "/home/airflow/gcs/data/salary.csv"
    DS_Salary.to_csv(salary_path,index = False)
    print(f"Output to {salary_path}")

   
def clean_data(salary_path,clean_path) :
    ## Read file csv from salarypath
    dt_pd = pd.read_csv(salary_path)
    ## Replace word 3 column:
        ## exp level
    dt_pd['experience_level'] = dt_pd['experience_level'].replace('MI','Mid-Level')
    dt_pd['experience_level'] = dt_pd['experience_level'].replace('SE','Senior')
    dt_pd['experience_level'] = dt_pd['experience_level'].replace('EX','Director')
    dt_pd['experience_level'] = dt_pd['experience_level'].replace('EN','Junior')
        ## company size
    dt_pd['company_size'] = dt_pd['company_size'].replace('S','Small')
    dt_pd['company_size'] = dt_pd['company_size'].replace('M','Medium')
    dt_pd['company_size'] = dt_pd['company_size'].replace('L','Large')
        ## emp_type
    dt_pd['employment_type'] = dt_pd['employment_type'].replace('FT','Full-time')
    dt_pd['employment_type'] = dt_pd['employment_type'].replace('CT','Contract')
    dt_pd['employment_type'] = dt_pd['employment_type'].replace('PT','Part-time')
    dt_pd['employment_type'] = dt_pd['employment_type'].replace('FL','Freelance')

    ## save file csv
    dt_pd.to_csv(clean_path,index = False)
    print(f"Output to {clean_path}")

with DAG(
    "saraly_102_final_dag",
    start_date = days_ago(1),
    schedule_interval = "@once",
    tags = ["project102"]
) as dag:

    ### Setup taks 1,2,3

    t1 = PythonOperator(
        task_id = "get_data_from_mysql",
        python_callable = get_data_from_mysql,
        op_kwargs = {"salary_path": get_data_from_mysql})

    t2 = PythonOperator(
        task_id = "clean_data",
        python_callable = clean_data,
        op_kwargs = {"salary_path":get_data_from_mysql,
                    "clean_path": clean_data})

    t3 = BashOperator(
        task_id = "load_to_bigquery",
        bash_command = "bq load \
            --source_format=CSV \
            --autodetect project102.DS_Salary\
             gs://asia-southeast1-project102-2fe5b97d-bucket/data/output102.csv",
    )
    
    t1 >> t2 >> t3
    
    

    
    




    
    

