# Databricks notebook source
import requests
from pyspark.sql import functions as F

# COMMAND ----------

def get_data(**kwargs):

    url = "https://api.opendota.com/api/proMatches"

    params = "&".join([f"{k}={v}" for k, v in kwargs.items() if k != 'df'])

    if params != "":
        url += "?" + params

    print(url)

    response = requests.get(url)

def get_min_match_id(df):
    # To get minimun id from match id
    # df.groupBy().agg(F.min("match_id")).collect()[0].asDict()['min(match_id)']   # One way to get
    min_match_id = df.groupBy().agg(F.min("match_id")).collect()[0][0]

    return min_match_id


# COMMAND ----------

def get_history_pro_matches(**kwargs):
    min_match_id = get_min_match_id(df)

    data = get_data(less_than_match_id=min_match_id)

# COMMAND ----------

df = spark.createDataFrame(response.json())
df.display()

# COMMAND ----------

df.coalesce(1).write.format("json").mode("append").save("/FileStore/tables/data/pro_matches_history")
