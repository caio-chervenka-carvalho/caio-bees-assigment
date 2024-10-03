print("\n\n\n\n\n Begin of Python Script \n\n")

"""
Details:
    • User             : Caio Chervenka de Carvalho
    • Project name     : Breweries Analytics
    • Date             : 2024-09-28
    • Script name      : 01_bronze_raw_data.py
    • Version          : v1 - 2024-09-28
    • Description      : Script design to extract the data from the API and store it on Bronze layer.



Code structure:
    1 - > Retrieve data:
            > Connect to API
                > IF successful (200) retrieve data as JSON ELSE try 5x times with 60 delay. IF all fail ABORT.
    2 - > Connect with the bucket:
            > Connect using the service account
            > Connect with the bucket
    3 - > Save the raw data:
            > Create the file with the following informations:
                > Format: "bees_brewery_raw_data_YYYYMMDD.json"
                > Path  : "breweries_analytics/raw/Input/"
    4 - > Finish code:
            > Return the success code "0"

"""

# Libraries
import requests
import sys
import time
import json
import datetime
from google.cloud import storage
from google.oauth2 import service_account



# 0 - Variables
api_url              = "https://api.openbrewerydb.org/breweries"
bucket_name          = "caio_bees_homework_assignment"
path                 = "breweries_analytics/Bronze Layer/input/"
file_name            = f"bees_brewery_raw_data_{datetime.datetime.today().strftime('%Y%m%d')}.json"
service_account_info = {"type": "service_account", "project_id": "learninggcp-91939", "private_key_id": "b971bc679c712d16dc71cf131165c26d24479b0b", "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCdqAgeRZbgXKbU\njf4HvAEjf8sODRan3td2UH3N6ebfvGHfvi5JomLmJG/dIwh95nf0kuoyuP+L5lUE\nnMBIGjnGVGiCAX2atU9IyEEHoCX7vVkEJuRAM/GO9k7ePmt2a/Vlmy6XzEPf4k3Q\nxfvGorL1cuP9qkvpzZ4uFVSTdRhgeCQ7/GqXoirzFiIx+94W8pe6FMzaSsV2Y8di\n7PEsETs0p6Ptu52WU4SrmBfCUBX1YXlic/BgcVfe2E3+kfJG1eqjXmEj5jNFmL/D\nZz14aRRMEWqqcgakSBHJdgcaGoI5nU0CVPmhNKr/Gyoc/XHwvgvhh372zIctntbU\nCO7Ag3ojAgMBAAECggEAANC07gpYSLCIXYWe2fjJnR6uWzqLf4wiKBn4p8WjkuYD\nllq1tl33gSiQelL4J19RB0ilNMrxHPUwqY8kwJqy6fwj9QKMXQe+ZIPbyW4CFEZ7\nFqlO0w6RtRzZjo7bEzTLgysYxBWWoKp00luoib/r1f1LzlMaazFlgoGyVMR6JX5a\nq2VJDFSDmvNMFAMwIHYlTABEsa0xmpGtWeT3m9w3o0fTsd41ZkqjPVkZSv4G7GUJ\nDp5cSQ/yFtRVTTLasLg9OGuNo65W1+rpQTDVFhJE0/wXKZJ8PyFNi+1Bq6C7wTjR\nP4qa/+ZvwVadGzC0Qle2NGN+ddFadzFn9ux7hEwsSQKBgQDYnMXOYSVJ3J+406Do\nHTbNmVfFsww6o6+qusOBhobm67wiVas+4iUBtjB8Gyy0+w+UdlbCs9DWMyqCnrcD\nzQBweBywkvIUxThSOulx1AYDubqJXWggQzVt4nhhwxjvmi/+AQLKYHtRe7+/LmSy\nS9hbOSeFRP96Q5SLzpb9AsW/aQKBgQC6UuDD7zU984/YtH6pMOW+r677QoXdkNkv\no3bvqebdijY7f4LpKnJlqRTwHJXiIduP0z+e9wJKPoJCxtj5040oFuGPTao5Wajj\n5ovwRobLfSyxqhDpKZcTrUbrmjuGHfmbLdkkEaLfdjyjzz2JS+9Sw1fbF7EwdTQZ\nEVg7B1LHqwKBgDikHfFOY8LBpx5ccFnhsb/nVhVDMXJv9PJu7cqD0+i0Qbi0sBe8\naLLe4iTBsloMwFEw9JTrEjPAo2AOgorC41eFgPMHKbgWrhiKgRqbt4rn2QgsXZr6\nGBGIw7PEoVOd/OiteP5UZkqzUjt6tSgOPx/zfQsTNL7SxxgbyE2WmQDpAoGAKx4O\nwYjTq1h3y1BJl2GnaO9C8QY98D4a5HlRhrisokvfrPkfFFIsUvZD0CTtUn0/UBM0\nv9atgvzgLA/UGkwoeESDQiRY5hinisXJUHshqml3NE4Ex9BGE9mfddolOC4rmwuL\nGluycz/rXIUS2njmEyL9a8gZyvr0aXCoGFDjiS0CgYA2c5I8DeXlRKpFfP5D98oA\n3F7AfYo0lTWP2Jm0GyPTK2EnXpEjp+Ne4VjkbxgHf1ygs1AtpYCG+Zgr1+we89CZ\nDxnRuurk+/dgcBC5tGqwLNeXoZ08xbQgCIo2uEdUFMOE0mw07vIF3aefWSyCKuzH\nzCfcHov1dcwbUoMmOcDRoQ==\n-----END PRIVATE KEY-----\n", "client_email": "bees-service-account@learninggcp-91939.iam.gserviceaccount.com", "client_id": "111977333792615950774", "auth_uri": "https://accounts.google.com/o/oauth2/auth", "token_uri": "https://oauth2.googleapis.com/token", "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs", "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/bees-service-account%40learninggcp-91939.iam.gserviceaccount.com", "universe_domain": "googleapis.com"}
counter              = 1



# 1 - Retrieve the data from the API
try:
    # 1.1 - Try to retrieve the data
    print("Connection the API...")
    response = requests.get(api_url)
    print(response)

    # 1.2 - Iteration loop to make 5 attempts to retrieve the data
    while (response.status_code != 200) and (counter <= 5):

        # 1.3 - Connecting API...
        response = requests.get(api_url)
        counter += 1
            
        # 1.4 - If status code != Give 5 attemps to retrieve the data 
        if response.status_code != 200:
            error_msg = f'Unable to retrieve the data from the API for the {counter}º attempt. Waiting 60 seconds to give it another try!!! \n\n'
            print(error_msg)
            time.sleep(60)

        # 1.5 - If exceed the number of attempts the process will be aborted
        elif counter == 5:
            error_msg = f"Error in request: Unable to retrieve the data after 5 attempts. Process aborted!!! \n\n"
            print(error_msg)
            sys.exit(1)

    # 1.6 - If status code == succesfull retrieve the data (json format)
    print("Successful connection! Retreiving the data...")
    data = response.json()

except:
    error_msg = f"Unable to retrieve data. URL corrupted. Process aborted!"
    sys.exit(1)



# 2 - Connect with the bucket
credentials = service_account.Credentials.from_service_account_info(service_account_info)
storage_client = storage.Client(credentials=credentials, project=service_account_info['project_id'])
bucket = storage_client.get_bucket(bucket_name)



# 3 - Save the file as JSON
# 3.1 - Create the blob file
blob = bucket.blob(f"{path}{file_name}")

# 3.2 - Upload the blob 
blob.upload_from_string(
    data=json.dumps(data),
    content_type='application/json'
    )



# 4 - Finish!
print(f"File '{file_name}' successfully save at the location: '{path}' . \n Module - 01_BRONZE_RAW_DATA finished. \n {datetime.datetime.today()}")
sys.exit(0)

# --------------------------------------------------------------------------------------------------------------------------------------------------