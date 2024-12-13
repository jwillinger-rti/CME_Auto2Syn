####################################
# Author: Jon Willinger
# Date: 2024-12-02
# Notes: Not for development in ide.
# For synapse.
####################################

import numpy as np
import pandas as pd
import requests
from pyspark.sql import SparkSession

data = {"Data_Set":["26 Crude Oil Last Day Financial Futures",
                    "B0 Mont Belvieu LDH Propane (OPIS) Futures",
                    "BZ Brent Crude Oil Last Day Financial Futures",
                    "C0 Mont Belvieu Ethane (OPIS) Futures",
                    "C1 Canadian Dollar/U.S. Dollar (CAD/USD) Futures",
                    "EC Euro/U.S. Dollar (EUR/USD) Futures",
                    "NG Henry Hub Natural Gas Futures"],
        "Month":["JAN25", "NOV24", "JAN25", "NOV24", "DEC24", "DEC24", "JAN25"],
        "Settlement_Price":[68.72000, 0.80444, 72.94, 0.19911, 0.7147, 1.05735, 3.363],
        "Last_Price":[np.NaN, np.NaN, 72.94, np.NaN, 0.7147, 1.05835, 3.368]
        }
df = pd.DataFrame(data=data)

####################################
####################################
####################################

def transform_df_for_azure_upsert(df):

    def _set_short_names(data_set):
        def __take_inverse(num: float) -> float:
            if num != 0.0:
                return 1/num;
            else: return num
        df = pd.DataFrame({});
        for n, data in enumerate(data_set):
            if data[1] == "26 Crude Oil Last Day Financial Futures":
                df["WTI Crude Oil"] = [data[3]] # Settlement
            elif data[1] == "B0 Mont Belvieu LDH Propane (OPIS) Futures":
                df["Propane"] = [data[3]] # Settlement
            elif data[1]  == "BZ Brent Crude Oil Last Day Financial Futures":
                df["Brent Crude Oil"] = [data[3]] # Settlement
            elif data[1]  == "C0 Mont Belvieu Ethane (OPIS) Futures":
                df["Ethane"] = [data[3]] # Settlement
            elif data[1]  == "C1 Canadian Dollar/U.S. Dollar (CAD/USD) Futures":
                df["US to CA$"] = [__take_inverse(float(data[4]))] # Last
            elif data[1]  == "EC Euro/U.S. Dollar (EUR/USD) Futures":
                df["Euro to $US"] = [data[4]] # Last
            elif data[1]  == "NG Henry Hub Natural Gas Futures":
                df["Nat. Gas"] = [data[3]] # Settlement
        return df

    records = df.to_records();
    df = _set_short_names(records)
    return df

####################################
####################################
####################################

# Transform df:
df_ = transform_df_for_azure_upsert(df)

# Define connection string parameters:
synapse_host = "rti-synapse-db.sql.azuresynapse.net"
port = 1433
db_table = "stg.Test_RTiPetchem_SO"
db_name = "synapsesqlserver"
linked_service_name = "rti_synapse_db_pyspark_uami01_ls"

# Get access token for jdbc connection authenticator:
# token = mssparkutils.credentials.getFullConnectionString(linked_service_name)
# TokenLibrary.help()
sql_access_token = TokenLibrary.getConnectionString(linked_service_name)

####################################
####################################
####################################

# Define connection string:
jdbc_url = f"jdbc:sqlserver://{synapse_host}:{port};database={db_name};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.sql.azuresynapse.net;loginTimeout=30"

# Define PySpark dataframe:
spark = SparkSession.builder.appName("ReadWriteToSynapseSQL").getOrCreate()
df_spark = spark.createDataFrame(df_)

# Define Read (here for reference):
# df_jdbc = spark.read \
# .format("jdbc") \
# .option("url", jdbc_url) \
# .option("accessToken", sql_access_token) \
# .option("dbtable", db_table) \
# .load()

####################################
####################################
####################################

# Write PySpark DataFrame into Synapse:

db_table2 = "stg.Test_RTiPetchem_SO2"

df_spark = df_spark \
    .withColumnRenamed("WTI Crude Oil", "wti_crude_oil") \
    .withColumnRenamed("Propane", "propane") \
    .withColumnRenamed("Brent Crude Oil", "brent_crude_oil") \
    .withColumnRenamed("Ethane", "ethane") \
    .withColumnRenamed("US to CA$", "usd2cad") \
    .withColumnRenamed("Euro to $US", "euro2usd") \
    .withColumnRenamed("Nat. Gas", "nat_gas") \

col_str = "wti_crude_oil FLOAT, propane FLOAT, brent_crude_oil FLOAT, ethane FLOAT, usd2cad FLOAT, euro2usd FLOAT, nat_gas FLOAT"

####################################
####################################
####################################

df_spark.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", db_table2) \
    .option("accessToken", sql_access_token) \
    .option("createTableColumnTypes", col_str) \
    .option("encrypt", "true") \
    .mode("overwrite") \
    .save() # could also append.

df_jdbc = spark.read \
.format("jdbc") \
.option("url", jdbc_url) \
.option("accessToken", sql_access_token) \
.option("dbtable", "stg.Test_RTiPetchem_SO2") \
.load()

####################################
####################################
####################################

