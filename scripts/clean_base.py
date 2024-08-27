import os
import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import TimestampNTZType

def create_spark_session():
    """Create and return a Spark session."""
    spark = (
        SparkSession.builder.appName("OptimizedSparkApp") 
        .config("spark.executor.memory", "4g") 
        .config("spark.driver.memory", "4g")  
        .config("spark.memory.fraction", "0.8") 
        .config("spark.memory.storageFraction", "0.5")  
        .config("spark.executor.cores", "2")  
        .config("spark.executor.instances", "3")  
        .config("spark.dynamicAllocation.enabled", "true")  
        .config("spark.dynamicAllocation.initialExecutors", "2")  
        .config("spark.dynamicAllocation.minExecutors", "1")  
        .config("spark.dynamicAllocation.maxExecutors", "10")  
        .config("spark.dynamicAllocation.executorIdleTimeout", "120s")  
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")  
        .config("spark.sql.shuffle.partitions", "200")  
        .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35")  
        .config("spark.sql.repl.eagerEval.enabled", True) 
        .config("spark.sql.parquet.cacheMetadata", "true") 
        .getOrCreate()
    )    
    return spark

def read_data(spark: SparkSession, directory_path: str, file_format='parquet'):
    """Reads all data from a directory with the specified file format."""
    return spark.read.format(file_format).load(directory_path)

def rename_column(df: DataFrame, old_name: list[str], new_name: list[str]):
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

def drop_invalid_negative_value(df: DataFrame, columns: list[str]):
    """Remove rows with negative values when they should be all >= 0"""
    for col in columns:
        df = df.filter(df[col] >= 0)
    return df

def drop_invalid_non_positive_value(df: DataFrame, columns: list[str]):
    """Remove rows with non_postive values when they should be all positive"""
    for col in columns:
        df = df.filter(df[col] > 0)
    return df
    
def drop_abnormal_high_values(df: DataFrame, column: str, upper_bound: float):
    """Drop records of column with values > upper_bound"""
    return df.filter(df[column] < upper_bound)

def drop_outliers(df: DataFrame, column: str, lower_bound: float, upper_bound: float):
    "Drop outliers of column"
    return df.filter(~((col(column) < lower_bound) | (col(column) > upper_bound)))

def sampling_data(df: DataFrame, column_name: str, fraction: float):
    """Stratified sampling data based on the distribution of the column"""
    # Create fractions dict for sampling
    fractions = {key: fraction for key in df.select(column_name).distinct().rdd.flatMap(lambda x: x).collect()}
    # Perform stratified sampling
    sampled_df = df.stat.sampleBy(column_name, fractions, seed=42)
    return sampled_df

def write_data(df: DataFrame, output_path: str, file_format='parquet'):
    """Writes the DataFrame back to a specified path using the given file format."""
    df.write.format(file_format).mode('overwrite').save(output_path)

def list_files_in_directory(directory_path: str):
    """Lists all files in a directory."""
    return [os.path.join(directory_path, f) for f in os.listdir(directory_path) if os.path.isfile(os.path.join(directory_path, f))]

def list_parquet_directories(directory_path: str):
    """Lists directories that likely contain parquet files."""
    directories = []
    for entry in os.listdir(directory_path):
        full_path = os.path.join(directory_path, entry)
        if os.path.isdir(full_path):
            directories.append(full_path)
    return directories
