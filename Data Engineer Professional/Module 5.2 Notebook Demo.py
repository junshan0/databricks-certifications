# Databricks notebook source
# MAGIC %md
# MAGIC Demonstration of importing modules within sys.path

# COMMAND ----------

import sys
print(sys.path)

# COMMAND ----------

from packages.test_sys import test_print

# COMMAND ----------

test_print()


# COMMAND ----------

# MAGIC %md
# MAGIC Demo of Notebook Scoped Library

# COMMAND ----------

import awscli

# COMMAND ----------

# MAGIC %pip install awscli

# COMMAND ----------

import awscli


# COMMAND ----------

# MAGIC %md
# MAGIC Databricks Utility - Widgets

# COMMAND ----------

dbutils.widgets.text("param_value", "100")

# COMMAND ----------

dbutils.widgets.getAll()

# COMMAND ----------

dbutils.widgets.get("param_value")

# COMMAND ----------

# MAGIC %md
# MAGIC Databricks Utility - Secrets

# COMMAND ----------

from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
w.secrets.create_scope(
    scope="sample_secret_scope",
    initial_manage_principal="users",
)
w.secrets.put_secret("sample_secret_scope","sample_secret_key",string_value ="password")

# COMMAND ----------

password = dbutils.secrets.get(scope = "sample_secret_scope", key = "sample_secret_key")
print(password)


# COMMAND ----------

for c in password:
    print(c)