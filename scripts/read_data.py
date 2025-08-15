import os
import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DataCleaning").getOrCreate()

RAW_PATH = "data/raw"

def read_csv(file_name):
    path = os.path.join(RAW_PATH, file_name)
    return spark.read.option("header", True).option("inferSchema", True).csv(path)

def read_txt(file_name, sep="\t"):
    path = os.path.join(RAW_PATH, file_name)
    return spark.read.option("header", True).option("inferSchema", True).text(path, sep=sep)

def read_xlsx(file_name):
    path = os.path.join(RAW_PATH, file_name)
    return pd.read_excel(path)
