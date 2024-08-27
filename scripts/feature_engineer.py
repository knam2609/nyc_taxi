from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import IntegerType, LongType
from pyspark.ml import Pipeline
from pyspark.sql.functions import year, month, dayofmonth, hour, minute, col

def prepare_timestamp_features(df: DataFrame, timestamp_col: str):
    """
    Splits a timestamp_ntz column into multiple features for linear regression.
    """
    df = df.withColumn(f"{timestamp_col}_year", year(col(timestamp_col)))
    df = df.withColumn(f"{timestamp_col}_month", month(col(timestamp_col)))
    df = df.withColumn(f"{timestamp_col}_day", dayofmonth(col(timestamp_col)))
    df = df.withColumn(f"{timestamp_col}_hour", hour(col(timestamp_col)))
    df = df.withColumn(f"{timestamp_col}_minute", minute(col(timestamp_col)))
    
    return df.drop(timestamp_col)


def encoder(df: DataFrame, column: str):
    """Encode categorical data"""
    is_integer_or_long = isinstance(df.schema[column].dataType, (IntegerType, LongType))
    print(is_integer_or_long)
    model = None
    if not is_integer_or_long:
        # If not integer or long, first index the strings
        indexer = StringIndexer(inputCol=column, outputCol=f"{column}_indexed")
        encoder = OneHotEncoder(inputCol=f"{column}_indexed", outputCol=f"{column}_encoded")
        pipeline = Pipeline(stages=[indexer, encoder])
        model = pipeline.fit(df)
        df = model.transform(df)
    else:
        # If it's already an integer or long, directly encode
        encoder = OneHotEncoder(inputCol=column, outputCol=f"{column}_encoded")
        model = encoder.fit(df)
        df = model.transform(df)

    df = df.drop(column)
    return df, model