# User: Caio Chervenka de Carvalho
# Version: 0.1.0
# Project name: Breweries Analytics
# Date: 2024-10-02

# 0 - Instructions
This project consists of a data processing pipeline divided into three Python scripts orchestrated by Airflow. The pipeline is set to run once a month at 6 AM and follows a sequential order, where the second script will only execute if the first one is successful, and the third will only run if the second one completes without errors. If any failure occurs, the process will be aborted. All of hte files are integrated with Google Cloud Platform (GCP).


# 1 - Project Structure
├── dags/
│   └── DAG_breweries_analytics_monthly_execution.py  # DAG script for Airflow
├── scripts/
│   ├── 01_bronze_raw_data.py        # First pipeline script
│   ├── 02_silver_parquet_data.py    # Second pipeline script
│   └── 03_gold_aggregated_data.py   # Third pipeline script
├── README.md                        # Instructions file


# 2 - Prerequisites
* Airflow Installation:

    Ensure Airflow is installed in your environment. You can install it using pip:
        pip install apache-airflow
    For more information, refer to the official Airflow documentation.

* Python Script Dependencies:

    The Python scripts require additional libraries. Run the following command to install the dependencies:
        # pip install google-cloud-storage
        # pip install gcsfs
        # pip install pyarrow
        # pip install requests
        # pip install json
        # pip install pandas

Make sure the Python scripts and the DAG file are placed in the correct Airflow directory. The DAG_breweries_analytics_monthly_execution.py file should be in the Airflow DAGs directory, for example:
    ~/airflow/dags/


# 3 - Execution Instructions
Copy the DAG and Python Script Files:

Place the DAG_breweries_anaçytics_monthly_execution.py file in the Airflow DAGs directory (typically ~/airflow/dags).
Put the 01_bronze_raw_data.py, 02_silver_parquet_data.py, and 03_gold_aggregated_data.py scripts in an accessible directory. The path to the scripts is configured in the DAG_breweries_anaçytics_monthly_execution.py file, so adjust it if needed.
Start Airflow:

To start Airflow, you can use the following commands:
airflow db init  # Initialize the Airflow database
airflow webserver --port 8080  # Start the web interface
airflow scheduler  # Start the DAG scheduler
Verify the DAG:

Access Airflow via the web interface (http://localhost:8080) and ensure the data_pipeline_dag appears in the list.
Enable the DAG by clicking the toggle button next to the DAG name.
Manual Trigger (Optional):

You can manually trigger the pipeline from the Airflow interface by clicking the "trigger" button (play icon).
Monitoring and Logs:

Track the DAG’s progress in the Airflow web interface. In case of failure, you can access detailed logs to check for specific errors in each task.
Error Handling
The pipeline is prepared to handle errors via try-except blocks in the Python scripts. If one of the scripts fails, the code will exit with status sys.exit(1), and the subsequent tasks in the DAG will not be executed. In Airflow, this will show as a task failure, stopping the execution of the remaining pipeline.


# 4 - Customization
You can adjust the following parameters in the DAG file:

    Execution Frequency:
        The DAG is currently set to run once a month at 6 AM (schedule_interval='0 6 1 * *'). You can modify this as per your requirements.

    Python Script Paths:
        Ensure that the paths to the scripts in the DAG file are correct for the environment where it is running.


# 5 - Observations
There were a couple of things that I would included if this was a real project, but since is for analysis only and theres probably pelnt of cases, I simplify a lot points. Belowe are all of the simplified points. And the next section will be about future improvements:

    1 - Google Cloud:
        > I created a bucket on Cloud Storage and created a service account with sufficient privilegies to execute all of the steps in our code. The right way (secure way) to do was to store the credentials on Airflow and authenticate the service account by reading the JSON file but since that would require another step I manually inserted the informations for the authentication on the script.
        > The same goes for bucket names and folders. I manually insert the variables but in the future it would be good to set those informations on Airflow direclty.
    2 - Python instead of PySPark:
        > Again, in order to optimize the execution I opt to use Pandas instead of pyspark. Pyspark would require a couple more steps for the instalation although with pyspark would be easier (for saving the partition tables and the speed of processing the data), for this use case I used only python.
    3 - Aggregated table:
        > I saved the output as a parquet table but ideally we should save it on BigQuery or similar technology so this data would be easy to use.
    4 - Error handling:
        > I use try catch blocks through all of the code so we can make sure that the if any error ocurs we can identify. Algo I gave possible message of what may be causing the issue.
    5 - Test case:
        > Since the pipeline uses an API as source I didnt add any use case but I did an script for validation. Seing the files on GCP and the final table.

# 6 - Future Improvements
Below are future improvements to be made:

    1 - Security:
        > Secure the code. By that I mean to implement the variables, any connection information (url, json file for the service account) and securely store it on Airflow and do not let any information seted on the code.
    2 - Containerization:
        > Execute the code using Docker (create an docker-compose.yml file) and adjust the code.
    3 - PySPark:
        > Migrate the script to PySpark so we can handle more data in the future and make the code easier. Specilly to handle the partitions.
    4 - Error and monitoring:
        > Implement monitoring tools (such as Cloud Monitoring) so we can have a depply understanding of the log and future improvements.
    5 - Partition:
        > The code was currently partition by the state and country (location) but it should as well for date. SInce I dont have the schedule for this process I didnt use any partition related to date bu the idea was to also partitionate by the file data (execution date).
    6 - Design pattern:
        > Create function for process and call the functions instead of the raw data.
    

# 7 - Special Thanks
I would like to express my appreciation to all the Ambev and Bees staff for their time and the opportunity. Also, a special thanks to you, the reader, who will execute this code. It is still in version 0.1, but I hope you enjoy it. If you have any comments or suggestions, please let me know.

Best regards, Caio Chervenka.