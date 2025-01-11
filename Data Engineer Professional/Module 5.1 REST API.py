# Databricks notebook source
databricks_token = '<databricks_token>'
databricks_host = '<databricks_host>'

# COMMAND ----------


import requests
headers = {
    "Authorization": f"Bearer {databricks_token}",
    "Accept": "application/json"
}
url = f'https://{databricks_host}/api/2.0/clusters/list'
response = requests.get(url, headers=headers)
print(response.json())

# COMMAND ----------

url = f'https://{databricks_host}/api/2.0/jobs/list'
response = requests.get(url, headers=headers)
print(response.json())

# COMMAND ----------

url = f'https://{databricks_host}/api/2.0/jobs/get'
data = '{ "job_id": "1234567890" }'
response = requests.get(url, data=data, headers=headers)
print(response.json())