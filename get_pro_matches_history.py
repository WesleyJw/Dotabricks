# Databricks notebook source
import requests
from pyspark.sql import functions as F

# COMMAND ----------

def get_data(**kwargs):

    url = "https://api.opendota.com/api/proMatches"

    params = "&".join([f"{k}={v}" for k, v in kwargs.items() if k != 'df'])

    if params != "":
        url += "?" + params

    response = requests.get(url)

    return response

def get_min_match_id(df):
    # To get minimun id from match id
    # df.groupBy().agg(F.min("match_id")).collect()[0].asDict()['min(match_id)']   # One way to get
    min_match_id = df.groupBy().agg(F.min("match_id")).collect()[0][0]

    return min_match_id

def get_history_pro_matches(**kwargs):
    
    response = get_data()
    df = spark.createDataFrame(response.json())
    min_match_id = get_min_match_id(df)

    # make first interaction
    data = get_data(less_than_match_id=min_match_id)

    return data


# COMMAND ----------

df1 = spark.createDataFrame(get_history_pro_matches().json())
df1.display()

# COMMAND ----------

#df.coalesce(1).write.format("json").mode("append").save("/FileStore/tables/data/pro_matches_history")
