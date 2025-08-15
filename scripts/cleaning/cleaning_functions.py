from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, lower

def drop_nulls(df: DataFrame, subset=None) -> DataFrame:
    """Drop rows with nulls in subset of columns"""
    return df.dropna(subset=subset)

def trim_strings(df: DataFrame) -> DataFrame:
    """Trim whitespace from all string columns"""
    string_cols = [field.name for field in df.schema.fields if str(field.dataType) == "StringType"]
    for c in string_cols:
        df = df.withColumn(c, trim(col(c)))
    return df

def normalize_case(df: DataFrame, cols=None) -> DataFrame:
    """Convert string columns to lowercase"""
    if not cols:
        cols = [field.name for field in df.schema.fields if str(field.dataType) == "StringType"]
    for c in cols:
        df = df.withColumn(c, lower(col(c)))
    return df

def remove_duplicates(df: DataFrame) -> DataFrame:
    return df.dropDuplicates()
