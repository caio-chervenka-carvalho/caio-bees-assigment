print("\n\n\n\n\n Begin of Python Script \n\n")

"""
Details:
    • User             : Caio Chervenka de Carvalho
    • Project name     : Breweries Analytics
    • Date             : 2024-09-28
    • Script name      : 03_gold_aggregated_table.py
    • Version          : v1 - 2024-09-28
    • Description      : Script design to transform the parquet table and provide usefull insights.



Code structure:
    1 - > Connect with Google:
            > Connect using the service account
            > Connect with the bucket
    2 - > Open parquet tables:
            > Create the schema using pyarrow to prevent any data loss or possible error on the importation
            > Loop to iterate between all of the parquet files saved. Open each parquet file individually (reduce errors with pandas. That wouldnt happen with pyspark) and append them into a list.
            > Create a single DF union all of the previous DFs (keep integrity)
    3 - > Create the aggregation table:
            > Create the count columns
            > Create the aggregated DF with the group by and count steps
    4 - > Save the aggregated table:
            > Save it as a parquet file on GOLD_LAYER folder
    5 - > Finish code:
            > Return the success code "0"

"""

# Libraries
import sys
import datetime
from google.cloud import storage
from google.oauth2 import service_account
import pandas as pd
import gcsfs
import pyarrow as pa



# 0 - Variables
bucket_name          = "caio_bees_homework_assignment"
parquet_path         = "breweries_analytics/Silver_Layer/parquet_table/"
aggregated_path      = "breweries_analytics/Gold_Layer/aggregated_table/"
service_account_info = {"type": "service_account", "project_id": "learninggcp-91939", "private_key_id": "b971bc679c712d16dc71cf131165c26d24479b0b", "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCdqAgeRZbgXKbU\njf4HvAEjf8sODRan3td2UH3N6ebfvGHfvi5JomLmJG/dIwh95nf0kuoyuP+L5lUE\nnMBIGjnGVGiCAX2atU9IyEEHoCX7vVkEJuRAM/GO9k7ePmt2a/Vlmy6XzEPf4k3Q\nxfvGorL1cuP9qkvpzZ4uFVSTdRhgeCQ7/GqXoirzFiIx+94W8pe6FMzaSsV2Y8di\n7PEsETs0p6Ptu52WU4SrmBfCUBX1YXlic/BgcVfe2E3+kfJG1eqjXmEj5jNFmL/D\nZz14aRRMEWqqcgakSBHJdgcaGoI5nU0CVPmhNKr/Gyoc/XHwvgvhh372zIctntbU\nCO7Ag3ojAgMBAAECggEAANC07gpYSLCIXYWe2fjJnR6uWzqLf4wiKBn4p8WjkuYD\nllq1tl33gSiQelL4J19RB0ilNMrxHPUwqY8kwJqy6fwj9QKMXQe+ZIPbyW4CFEZ7\nFqlO0w6RtRzZjo7bEzTLgysYxBWWoKp00luoib/r1f1LzlMaazFlgoGyVMR6JX5a\nq2VJDFSDmvNMFAMwIHYlTABEsa0xmpGtWeT3m9w3o0fTsd41ZkqjPVkZSv4G7GUJ\nDp5cSQ/yFtRVTTLasLg9OGuNo65W1+rpQTDVFhJE0/wXKZJ8PyFNi+1Bq6C7wTjR\nP4qa/+ZvwVadGzC0Qle2NGN+ddFadzFn9ux7hEwsSQKBgQDYnMXOYSVJ3J+406Do\nHTbNmVfFsww6o6+qusOBhobm67wiVas+4iUBtjB8Gyy0+w+UdlbCs9DWMyqCnrcD\nzQBweBywkvIUxThSOulx1AYDubqJXWggQzVt4nhhwxjvmi/+AQLKYHtRe7+/LmSy\nS9hbOSeFRP96Q5SLzpb9AsW/aQKBgQC6UuDD7zU984/YtH6pMOW+r677QoXdkNkv\no3bvqebdijY7f4LpKnJlqRTwHJXiIduP0z+e9wJKPoJCxtj5040oFuGPTao5Wajj\n5ovwRobLfSyxqhDpKZcTrUbrmjuGHfmbLdkkEaLfdjyjzz2JS+9Sw1fbF7EwdTQZ\nEVg7B1LHqwKBgDikHfFOY8LBpx5ccFnhsb/nVhVDMXJv9PJu7cqD0+i0Qbi0sBe8\naLLe4iTBsloMwFEw9JTrEjPAo2AOgorC41eFgPMHKbgWrhiKgRqbt4rn2QgsXZr6\nGBGIw7PEoVOd/OiteP5UZkqzUjt6tSgOPx/zfQsTNL7SxxgbyE2WmQDpAoGAKx4O\nwYjTq1h3y1BJl2GnaO9C8QY98D4a5HlRhrisokvfrPkfFFIsUvZD0CTtUn0/UBM0\nv9atgvzgLA/UGkwoeESDQiRY5hinisXJUHshqml3NE4Ex9BGE9mfddolOC4rmwuL\nGluycz/rXIUS2njmEyL9a8gZyvr0aXCoGFDjiS0CgYA2c5I8DeXlRKpFfP5D98oA\n3F7AfYo0lTWP2Jm0GyPTK2EnXpEjp+Ne4VjkbxgHf1ygs1AtpYCG+Zgr1+we89CZ\nDxnRuurk+/dgcBC5tGqwLNeXoZ08xbQgCIo2uEdUFMOE0mw07vIF3aefWSyCKuzH\nzCfcHov1dcwbUoMmOcDRoQ==\n-----END PRIVATE KEY-----\n", "client_email": "bees-service-account@learninggcp-91939.iam.gserviceaccount.com", "client_id": "111977333792615950774", "auth_uri": "https://accounts.google.com/o/oauth2/auth", "token_uri": "https://oauth2.googleapis.com/token", "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs", "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/bees-service-account%40learninggcp-91939.iam.gserviceaccount.com", "universe_domain": "googleapis.com"}



# 1 - Connect with Google
try:
    scopes = ["https://www.googleapis.com/auth/cloud-platform"]
    credentials = service_account.Credentials.from_service_account_info( service_account_info, scopes=scopes)
    storage_client = storage.Client(credentials=credentials, project=service_account_info['project_id'])
    bucket = storage_client.get_bucket(bucket_name)
    fs = gcsfs.GCSFileSystem(token=credentials)

except:
    print(f"Unable to connect with Google. Please verify credentials and path. Process aborted!")
    sys.exit(1)



# 2 - Open parquet table
try:
    # 2.1 - Create the table schema using pyarrow to prevent errors with the data
    schema = pa.schema([
        ('id', pa.string()),
        ('name', pa.string()),
        ('brewery_type', pa.string()),
        ('address_1', pa.string()),
        ('address_2', pa.string()),
        ('address_3', pa.string()),
        ('city', pa.string()),
        ('state_province', pa.string()),
        ('postal_code', pa.string()),
        ('country', pa.string()),
        ('longitude', pa.string()),
        ('latitude', pa.string()),
        ('phone', pa.string()),
        ('website_url', pa.string()),
        ('state', pa.string()),
        ('street', pa.string()),
        ('filename', pa.string()),
        ('ingestion_date', pa.string()),
        ('ingestion_timestamp', pa.string())])
    
    # 2.2 - List all of the files inside the folder ad open each parquet file separated from each other to prevent any error during the importstion. Observation: PySpark wouldnt have this issue!
    blobs = bucket.list_blobs(prefix=parquet_path)
    df_list = []
    for blob in blobs:
        if "data.parquet" in blob.name:
                df_list.append(pd.read_parquet(f"gs://{bucket_name}/{blob.name}", filesystem=fs, schema=schema))

    # 2.3 - Concatenate all of the DataFrames into a single one containing all of the data
    df = pd.concat(df_list)

except:
    print(f"Unable to import the parquet files. Please verify credentials, permissions, parquet path and parquet file. Process aborted!")
    sys.exit(1)



# 3 - Create aggregated table based on location (state and country) and brewery tyepe
try:
    # 3.1 - Create a count columns
    df['count'] = 1

    # 3.2 - Create the dataframe with the aggregated count
    df_aggregated = df.groupby(['brewery_type', 'state', 'country'])['count'].count().reset_index()

except:
    print(f"Unable to generate the query. Please verify the table schema and sintaxe. Process aborted!")
    sys.exit(1)



# 4 - Save aggregated table as a parquet file on GOLD LAYER folder
try:
    with fs.open(f"gs://{bucket_name}/{aggregated_path}", 'wb') as f:
            df_aggregated.to_parquet(f, engine='pyarrow', index=False)
    print(f"Aggregated table successfully processed at the location: '{aggregated_path}' . \n Module - 03_GOLD_AGGREGATED_TABLE finished. \n {datetime.datetime.today()}")

except:
    print(f"Unable to save the table. Please verify the path, if exists a table there already (the process is not set to overwrite nor to append) and sintaxe. Process aborted!")
    sys.exit(1)


# 5 - Finish!
sys.exit(0)
# --------------------------------------------------------------------------------------------------------------------------------------------------