print("\n\n\n\n\n Begin of Python Script \n\n")

"""
Details:
    • User             : Caio Chervenka de Carvalho
    • Project name     : Breweries Analytics
    • Date             : 2024-09-28
    • Script name      : 02_silver_parquet_table.py
    • Version          : v1 - 2024-09-28
    • Description      : Script design to transform the raw data and and store it on Silver layer as a parquet table.



Code structure:
    1 - > Connect with Google:
            > Connect using the service account
            > Connect with the bucket
    2 - > Retrieve the data:
            > List all of the files inside the input folder
            > IF the file has the same pattern THEN append into a list to be processed later
    3 - > Open Dataframe:
            > Transform the JSON file into a Pandas Dataframe
    4 - > Transform the dataframe:
            > Open as dataframe
            > Transform:
                    > Set all columns to lower
                    > Create ingestion control columns (filename, ingestion_date and ingestion_timestamp)
            > Save into a parquet file partitioned by two columns = country and state (in pyspark is easier)
    5 - > Move raw data to processed folder:
            > Copy raw file to processed folder
            > Delete raw file from the input file
    6 - > Finish code:
            > Return the success code "0"

"""

# Libraries
import sys
import datetime
from google.cloud import storage
from google.oauth2 import service_account
import re
import pandas as pd
import json
import gcsfs
import pyarrow



# 0 - Variables
bucket_name          = "caio_bees_homework_assignment"
input_path           = "breweries_analytics/Bronze Layer/input/"
processed_path       = "breweries_analytics/Bronze Layer/processed/"
parquet_path         = "breweries_analytics/Silver_Layer/parquet_table/"
service_account_info = {"type": "service_account", "project_id": "learninggcp-91939", "private_key_id": "b971bc679c712d16dc71cf131165c26d24479b0b", "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCdqAgeRZbgXKbU\njf4HvAEjf8sODRan3td2UH3N6ebfvGHfvi5JomLmJG/dIwh95nf0kuoyuP+L5lUE\nnMBIGjnGVGiCAX2atU9IyEEHoCX7vVkEJuRAM/GO9k7ePmt2a/Vlmy6XzEPf4k3Q\nxfvGorL1cuP9qkvpzZ4uFVSTdRhgeCQ7/GqXoirzFiIx+94W8pe6FMzaSsV2Y8di\n7PEsETs0p6Ptu52WU4SrmBfCUBX1YXlic/BgcVfe2E3+kfJG1eqjXmEj5jNFmL/D\nZz14aRRMEWqqcgakSBHJdgcaGoI5nU0CVPmhNKr/Gyoc/XHwvgvhh372zIctntbU\nCO7Ag3ojAgMBAAECggEAANC07gpYSLCIXYWe2fjJnR6uWzqLf4wiKBn4p8WjkuYD\nllq1tl33gSiQelL4J19RB0ilNMrxHPUwqY8kwJqy6fwj9QKMXQe+ZIPbyW4CFEZ7\nFqlO0w6RtRzZjo7bEzTLgysYxBWWoKp00luoib/r1f1LzlMaazFlgoGyVMR6JX5a\nq2VJDFSDmvNMFAMwIHYlTABEsa0xmpGtWeT3m9w3o0fTsd41ZkqjPVkZSv4G7GUJ\nDp5cSQ/yFtRVTTLasLg9OGuNo65W1+rpQTDVFhJE0/wXKZJ8PyFNi+1Bq6C7wTjR\nP4qa/+ZvwVadGzC0Qle2NGN+ddFadzFn9ux7hEwsSQKBgQDYnMXOYSVJ3J+406Do\nHTbNmVfFsww6o6+qusOBhobm67wiVas+4iUBtjB8Gyy0+w+UdlbCs9DWMyqCnrcD\nzQBweBywkvIUxThSOulx1AYDubqJXWggQzVt4nhhwxjvmi/+AQLKYHtRe7+/LmSy\nS9hbOSeFRP96Q5SLzpb9AsW/aQKBgQC6UuDD7zU984/YtH6pMOW+r677QoXdkNkv\no3bvqebdijY7f4LpKnJlqRTwHJXiIduP0z+e9wJKPoJCxtj5040oFuGPTao5Wajj\n5ovwRobLfSyxqhDpKZcTrUbrmjuGHfmbLdkkEaLfdjyjzz2JS+9Sw1fbF7EwdTQZ\nEVg7B1LHqwKBgDikHfFOY8LBpx5ccFnhsb/nVhVDMXJv9PJu7cqD0+i0Qbi0sBe8\naLLe4iTBsloMwFEw9JTrEjPAo2AOgorC41eFgPMHKbgWrhiKgRqbt4rn2QgsXZr6\nGBGIw7PEoVOd/OiteP5UZkqzUjt6tSgOPx/zfQsTNL7SxxgbyE2WmQDpAoGAKx4O\nwYjTq1h3y1BJl2GnaO9C8QY98D4a5HlRhrisokvfrPkfFFIsUvZD0CTtUn0/UBM0\nv9atgvzgLA/UGkwoeESDQiRY5hinisXJUHshqml3NE4Ex9BGE9mfddolOC4rmwuL\nGluycz/rXIUS2njmEyL9a8gZyvr0aXCoGFDjiS0CgYA2c5I8DeXlRKpFfP5D98oA\n3F7AfYo0lTWP2Jm0GyPTK2EnXpEjp+Ne4VjkbxgHf1ygs1AtpYCG+Zgr1+we89CZ\nDxnRuurk+/dgcBC5tGqwLNeXoZ08xbQgCIo2uEdUFMOE0mw07vIF3aefWSyCKuzH\nzCfcHov1dcwbUoMmOcDRoQ==\n-----END PRIVATE KEY-----\n", "client_email": "bees-service-account@learninggcp-91939.iam.gserviceaccount.com", "client_id": "111977333792615950774", "auth_uri": "https://accounts.google.com/o/oauth2/auth", "token_uri": "https://oauth2.googleapis.com/token", "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs", "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/bees-service-account%40learninggcp-91939.iam.gserviceaccount.com", "universe_domain": "googleapis.com"}
pattern              = r"bees_brewery_raw_data_\d{8}\.json"
files_list           = []



# 1 - Connect with Google
try:
    scopes = ["https://www.googleapis.com/auth/cloud-platform"]
    credentials = service_account.Credentials.from_service_account_info( service_account_info, scopes=scopes)
    storage_client = storage.Client(credentials=credentials, project=service_account_info['project_id'])
    bucket = storage_client.get_bucket(bucket_name)
    fs = gcsfs.GCSFileSystem(token=credentials)

except:
    error_msg = f"Unable to connect with Google. Please verify credentials and path. Process aborted!"
    sys.exit(1)



# 2 - Retrieve the data
try:
    # 2.1 - Get all of the files from the folder
    blobs = storage_client.list_blobs(bucket_name, prefix=input_path)

    # 2.2 - Get the file based on the file name pattern
    for file in blobs:
        if re.search(pattern, file.name.lower()):
            files_list.append(file)
except:
    error_msg = f"Unable to connect with Google and list the files. Please verify credentials permissions to see if you have the right role and permission to access the bucket. Process aborted!"
    sys.exit(1)



# loop through all of the avaliable files
for file in files_list:
    try:
        # 3.0 - Get the variables and downalod the JSON as string and transform it into a dictionary
        file_name = file.name.upper()
        content   = file.download_as_string()
        data      = json.loads(content)

        # 3.1 - Open as DataFrame
        df = pd.DataFrame(data)
    except:
        error_msg = f"Unable to open the JSON file. File may be corrupted or wrong path. Please verify both informations. Process aborted!"
        sys.exit(1)
        

    # 4 - Transform the dataframe
    # 4.1 - Transform the columns - Set all of their names to lower
    df.rename(columns={column: column.lower() for column in df.columns}, inplace=True)
    
    # 4.2 - Add columns
    df['filename']            = file_name.split("/")[-1]
    df['ingestion_date']      = datetime.datetime.today().strftime("%Y-%m-%d")
    df['ingestion_timestamp'] = datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S")
    df['country']             = df['country'].str.replace(' ', '_').str.lower()
    df['state']               = df['state'].str.replace(' ', '_').str.lower()

    
    try:
        # 4.3 - Save the data as parquet (Loop requeried for Pandas. With PySpark theres no need to create the loop. Simply put= df.write.partitionBy("country", "state").parquet("gs_path"))
        for (country, state), group_df in df.groupby(['country', 'state']):
            # 2.3.1 - File Path
            gcs_path = f"gs://{bucket_name}/{parquet_path}country={country}/state={state}/data.parquet"
            
            # 2.3.2 - Save the data frame on GCP
            with fs.open(gcs_path, 'wb') as f:
                group_df.to_parquet(f, engine='pyarrow', index=False)
            print(f"Partition country:{country} - state={state} successfully saved at {gcs_path}!")
    except:
        error_msg = f"Unable to save the dataframe as a parquet file. Data may be corrupted or the service account does not have sufficient permissions. Please verify both informations. Process aborted!"
        sys.exit(1)


    # 4 - Move raw data to processed folder
    # 4.1 - Copy the source file the processed folder
    new_blob = bucket.copy_blob(
        file, bucket, f"{processed_path}{file.name.split("/")[-1]}")
    
    # 4.2 - Delete file from the input folder
    file.delete()
    print(f"File '{file.name}' successfully processed at the location: '{parquet_path}' . \n Module - 02_SILVER_PARQUET_TABLE finished. \n {datetime.datetime.today()}")



# 5 - Finish!
sys.exit(0)

# --------------------------------------------------------------------------------------------------------------------------------------------------