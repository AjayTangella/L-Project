# Databricks notebook source
# MAGIC %md
# MAGIC #Bronze layer mount

# COMMAND ----------

configs = {
  "fs.azure.account.auth.type": "OAuth",
  "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id": "54f49301-ce6e-41f9-82a0-900cb772d7aa",
  "fs.azure.account.oauth2.client.secret": "Gmj8Q~bSaKHpqrFRpryI.DUcF58Y2Ujhmb2OjaLc",
  "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/5b8f491e-4680-4384-8d7f-c2bde51ca7c5/oauth2/token"
}

dbutils.fs.mount(
  source = "abfss://bronze@awsprojectdatalake.dfs.core.windows.net/",
  mount_point = "/mnt/bronze",
  extra_configs = configs
)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/bronze

# COMMAND ----------

# MAGIC %md
# MAGIC #silver Layer mount

# COMMAND ----------

configs = {
  "fs.azure.account.auth.type": "OAuth",
  "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id": "54f49301-ce6e-41f9-82a0-900cb772d7aa",
  "fs.azure.account.oauth2.client.secret": "Gmj8Q~bSaKHpqrFRpryI.DUcF58Y2Ujhmb2OjaLc",
  "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/5b8f491e-4680-4384-8d7f-c2bde51ca7c5/oauth2/token"
}

dbutils.fs.mount(
  source = "abfss://silver@awsprojectdatalake.dfs.core.windows.net/",
  mount_point = "/mnt/silver",
  extra_configs = configs
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Read file from broze layer

# COMMAND ----------

df_
