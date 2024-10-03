print("\n\n\n\n\n Begin of DAG execution \n\n")

"""
Details:
    • User             : Caio Chervenka de Carvalho
    • Project name     : Breweries Analytics
    • Date             : 2024-09-2
    • Script name      : DAG_breweries_analytics_monthly_execution.py
    • Version          : v1 - 2024-09-28
    • Description      : Script design to orchestrate the Brewery Analytics project.



Code structure:
    1 - > Run script function:
            > CInsert the subprocess to handle possible errors
    2 - > Create DAG:
            > Parameters:
                    > Retries set to 2 attempts
                    > No dependency with previous executions
            > Tasks:
                    > 1 - Run first script
                    > 2 - Run second script depending of the execution of the first DAG. If the first script fails the process will be aborted.
                    > 3 - Run third script depending of the execution of the second DAG. If the second script fails the process will be aborted.
            > Dependencies:
                    > 01_bronze_raw_data.py >> 02_silver_parquet_data.py >> 03_gold_aggregated_data.py
    3 - > Finish code:
            > End the DAG flow

"""

# Libraries
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess



# 1 - Function: Run script with an error raise
def run_script(script_name):
    try:
        # Execute the process with the subprocess RUN.
        result = subprocess.run(['python3', script_name], check=True)
        print(f"{script_name} executed successfully.")
    except subprocess.CalledProcessError as e:
        # If the execution fails, the error is raised.
        print(f"Error running {script_name}: {e}")
        raise

# 2 - Create the DAG with 2 retries
default_args = {
    'owner': 'caio',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

with DAG(
    'monthly_pipeline',
    default_args=default_args,
    description='DAG to run Python scripts sequentially with error handling',
    schedule_interval='0 6 1 * *',  # Execute the DAG at the first day of the month at 06h00AM
    start_date=datetime(2024, 10, 1),  # Begin date
    catchup=False,  # Do not execute previous DAGs
) as dag:

    # Task 1 : Execute the first script
    task_1 = PythonOperator(
        task_id='bronze_raw_data_module',
        python_callable=run_script,
        op_args=['scripts/01_bronze_raw_data.py'],
    )

    # Task 2 : Execute the second depending of the execution of the first script.
    task_2 = PythonOperator(
        task_id='silver_parquet_data_module',
        python_callable=run_script,
        op_args=['/scripts/02_silver_parquet_data.py'],
    )

    # Task 3 : Execute the third depending of the execution of the second script.
    task_3 = PythonOperator(
        task_id='gold_aggregated_data_module',
        python_callable=run_script,
        op_args=['scripts/03_gold_aggregated_data.py'],
    )

    # Creating the dependencies of th tasks ( ">>" )
    task_1 >> task_2 >> task_3


# Finish!
# --------------------------------------------------------------------------------------------------------------------------------------------------