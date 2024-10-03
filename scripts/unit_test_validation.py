print("\n\n\n\n\n Begin of Python Script \n\n")

"""
Details:
    • User             : Caio Chervenka de Carvalho
    • Project name     : Breweries Analytics
    • Date             : 2024-09-28
    • Script name      : unit_test_validation.py
    • Version          : v1 - 2024-09-28
    • Description      : Script design to see each stage of the scripts to validate the process.



Code structure:
    1 - > Show the folder structure of the BRONZE layer
    2 - > Show the folder structure of the SILVER layer
    3 - > Show the folder structure of the GOLD layer
    4 - > Show the Aggregated table (df)
    5 - > Finish code:

"""


# Libraries
from google.cloud import storage
from google.oauth2 import service_account
import pandas as pd
import gcsfs
import pyarrow as pa



# 0 - Variables
bucket_name         = "caio_bees_homework_assignment"
bronze_path         = "breweries_analytics/Bronze Layer/"
silver_path         = "breweries_analytics/Silver_Layer/"
gold_path           = "breweries_analytics/Gold_Layer/"
service_account_info = {"type": "service_account", "project_id": "learninggcp-91939", "private_key_id": "b971bc679c712d16dc71cf131165c26d24479b0b", "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCdqAgeRZbgXKbU\njf4HvAEjf8sODRan3td2UH3N6ebfvGHfvi5JomLmJG/dIwh95nf0kuoyuP+L5lUE\nnMBIGjnGVGiCAX2atU9IyEEHoCX7vVkEJuRAM/GO9k7ePmt2a/Vlmy6XzEPf4k3Q\nxfvGorL1cuP9qkvpzZ4uFVSTdRhgeCQ7/GqXoirzFiIx+94W8pe6FMzaSsV2Y8di\n7PEsETs0p6Ptu52WU4SrmBfCUBX1YXlic/BgcVfe2E3+kfJG1eqjXmEj5jNFmL/D\nZz14aRRMEWqqcgakSBHJdgcaGoI5nU0CVPmhNKr/Gyoc/XHwvgvhh372zIctntbU\nCO7Ag3ojAgMBAAECggEAANC07gpYSLCIXYWe2fjJnR6uWzqLf4wiKBn4p8WjkuYD\nllq1tl33gSiQelL4J19RB0ilNMrxHPUwqY8kwJqy6fwj9QKMXQe+ZIPbyW4CFEZ7\nFqlO0w6RtRzZjo7bEzTLgysYxBWWoKp00luoib/r1f1LzlMaazFlgoGyVMR6JX5a\nq2VJDFSDmvNMFAMwIHYlTABEsa0xmpGtWeT3m9w3o0fTsd41ZkqjPVkZSv4G7GUJ\nDp5cSQ/yFtRVTTLasLg9OGuNo65W1+rpQTDVFhJE0/wXKZJ8PyFNi+1Bq6C7wTjR\nP4qa/+ZvwVadGzC0Qle2NGN+ddFadzFn9ux7hEwsSQKBgQDYnMXOYSVJ3J+406Do\nHTbNmVfFsww6o6+qusOBhobm67wiVas+4iUBtjB8Gyy0+w+UdlbCs9DWMyqCnrcD\nzQBweBywkvIUxThSOulx1AYDubqJXWggQzVt4nhhwxjvmi/+AQLKYHtRe7+/LmSy\nS9hbOSeFRP96Q5SLzpb9AsW/aQKBgQC6UuDD7zU984/YtH6pMOW+r677QoXdkNkv\no3bvqebdijY7f4LpKnJlqRTwHJXiIduP0z+e9wJKPoJCxtj5040oFuGPTao5Wajj\n5ovwRobLfSyxqhDpKZcTrUbrmjuGHfmbLdkkEaLfdjyjzz2JS+9Sw1fbF7EwdTQZ\nEVg7B1LHqwKBgDikHfFOY8LBpx5ccFnhsb/nVhVDMXJv9PJu7cqD0+i0Qbi0sBe8\naLLe4iTBsloMwFEw9JTrEjPAo2AOgorC41eFgPMHKbgWrhiKgRqbt4rn2QgsXZr6\nGBGIw7PEoVOd/OiteP5UZkqzUjt6tSgOPx/zfQsTNL7SxxgbyE2WmQDpAoGAKx4O\nwYjTq1h3y1BJl2GnaO9C8QY98D4a5HlRhrisokvfrPkfFFIsUvZD0CTtUn0/UBM0\nv9atgvzgLA/UGkwoeESDQiRY5hinisXJUHshqml3NE4Ex9BGE9mfddolOC4rmwuL\nGluycz/rXIUS2njmEyL9a8gZyvr0aXCoGFDjiS0CgYA2c5I8DeXlRKpFfP5D98oA\n3F7AfYo0lTWP2Jm0GyPTK2EnXpEjp+Ne4VjkbxgHf1ygs1AtpYCG+Zgr1+we89CZ\nDxnRuurk+/dgcBC5tGqwLNeXoZ08xbQgCIo2uEdUFMOE0mw07vIF3aefWSyCKuzH\nzCfcHov1dcwbUoMmOcDRoQ==\n-----END PRIVATE KEY-----\n", "client_email": "bees-service-account@learninggcp-91939.iam.gserviceaccount.com", "client_id": "111977333792615950774", "auth_uri": "https://accounts.google.com/o/oauth2/auth", "token_uri": "https://oauth2.googleapis.com/token", "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs", "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/bees-service-account%40learninggcp-91939.iam.gserviceaccount.com", "universe_domain": "googleapis.com"}



# 0.1 - Connect with Google
scopes = ["https://www.googleapis.com/auth/cloud-platform"]
credentials = service_account.Credentials.from_service_account_info( service_account_info, scopes=scopes)
storage_client = storage.Client(credentials=credentials, project=service_account_info['project_id'])
bucket = storage_client.get_bucket(bucket_name)
fs = gcsfs.GCSFileSystem(token=credentials)



# 1 - Validation for the first script - BRONZE
print("\n Validation for the script 01 - BRONZE RAW DATA \n Data files structure on Cloud Storage")
counter = 0
print("""├── breweries_analytics/
│   └── Bronze Layer/""")
a = [i.name for i in bucket.list_blobs(prefix=bronze_path)]
for i in a:
    counter += 1
    if i.replace(bronze_path, "").count("/") >= 1:

        if counter == len(a):
            print(f"""│       └── {i.replace(bronze_path, "")}""")
        
        else:
            print(f"""│       ├── {i.replace(bronze_path, "")}""")
print("\n\n\n")




# 2 - Validation for the second script - SILVER
print("\n Validation for the script 02 - SILVER PARQUET DATA \n Data files structure on Cloud Storage")
counter = 0
print("""├── breweries_analytics/
│   └── Silver_Layer/""")
a = [i.name for i in bucket.list_blobs(prefix=silver_path)]
for i in a:
    counter += 1
    if i.replace(silver_path, "").count("/") >= 1:

        if counter == len(a):
            print(f"""│       └── {i.replace(silver_path, "")}""")
        
        else:
            print(f"""│       ├── {i.replace(silver_path, "")}""")
print("\n\n\n")



# 3 - Validation for the third script - GOLD 
# 3.1 - Validate files
print("""├── breweries_analytics/
│   └── Gold_Layer/""")
counter = 0
a = [i.name for i in bucket.list_blobs(prefix=gold_path)]
for i in a:
    if "data.parquet" in blob.name:
        agg_path = f"gs://{bucket_name}/{blob.name}"

    counter += 1

    if i.replace(gold_path, "").count("/") >= 1:

        if counter == len(a):
            print(f"""│       └── {i.replace(gold_path, "")}""")
        
        else:
            print(f"""│       ├── {i.replace(gold_path, "")}""")

# 3.2 - Validate table
df = pd.read_parquet(agg_path, filesystem=fs)
df.head(10)



print("Finish")
# ----------------------------------------------------------------------------------------------------------