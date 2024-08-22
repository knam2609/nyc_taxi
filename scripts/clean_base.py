import os
import datetime
from typing import List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import TimestampNTZType

def create_spark_session():
    """Create and return a Spark session."""
    spark = SparkSession.builder \
        .appName("Project 1") \
        .config("spark.executor.memory", "8g") \
        .getOrCreate()    
    return spark

def read_data(spark: SparkSession, directory_path: str, file_format='parquet'):
    """Reads all data from a directory with the specified file format."""
    return spark.read.format(file_format).load(directory_path)

def rename_column(df: DataFrame, old_name: List[str], new_name: List[str]):
    """Rename columns"""
    for i in range(len(old_name)):
        df = df.withColumnRenamed(old_name[i], new_name[i])
    return df

def drop_null_values(df: DataFrame):
    """Removes rows with any null values from the DataFrame."""
    return df.na.drop()

def drop_invalid_locationID(df:DataFrame, taxi_zones: DataFrame):
    """Remove rows with any invalid location ID"""
    valid_zones = set([row['LocationID'] for row in taxi_zones.select("LocationID").distinct().collect()])
    return df.filter(col("PUlocationID").isin(valid_zones) & col("DOlocationID").isin(valid_zones))

def find_valid_time_range(file_name: str):
    """Find valid time range (1 month) from file name"""
    # Extract the date part from the filename
    date_part = file_name.split('/')[-1].split('.')[0]  # This should give 'yyyy-mm'
    # Parse the year and month
    year, month = map(int, date_part.split('-'))
    # Calculate the first and last day of the month
    start_time = datetime.datetime(year, month, 1)  # Start of the month
    end_time = datetime.datetime(year, month + 1, 1) - datetime.timedelta(seconds=1)  # End of the month
    # Format as strings
    start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
    end_time_str = end_time.strftime('%Y-%m-%d %H:%M:%S')
    print("Start time:", start_time_str)
    print("End time:", end_time_str)
    return start_time_str, end_time_str

def drop_invalid_time(df: DataFrame, start_time: str, end_time: str):
    """Remove rows with any out of range datetime"""
    # Find all datetime columns
    datetime_columns = [f.name for f in df.schema.fields if isinstance(f.dataType, TimestampNTZType)]
    # Filter every columns with datetime
    for column in datetime_columns:
        df = df.filter(col(column).between(start_time, end_time))
        print(f"Filtered data for column {column}:")
    return df

def drop_invalid_non_positive_value(df: DataFrame, column_name: str):
    """Remove rows with non-positive values when they should be all positive"""
    return df.filter(df[column_name] > 0)

def sampling_data(df: DataFrame, column_name: str, subset: float):
    """Stratified sampling data based on the distribution of the column"""
    # Count each HVFHV license number and calculate fractions for sampling
    distinct_values = df.select(column_name).distinct().collect()
    #total_count = df.count()
    fractions = {row[column_name]: subset for row in distinct_values}
    print(fractions)
    # Perform stratified sampling
    sampled_df = df.stat.sampleBy(column_name, fractions, seed=42)
    return sampled_df

def write_data(df: DataFrame, output_path: str, file_format='parquet'):
    """Writes the DataFrame back to a specified path using the given file format."""
    df.write.format(file_format).mode('overwrite').save(output_path)

def list_files_in_directory(directory_path: str):
    """Lists all files in a directory."""
    return [os.path.join(directory_path, f) for f in os.listdir(directory_path) if os.path.isfile(os.path.join(directory_path, f))]
