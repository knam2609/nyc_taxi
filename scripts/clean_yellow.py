from . import clean_base
from .manipulate_data import detect_outliers
from pyspark.sql.functions import col, unix_timestamp

VENDOR_ID = [1, 2]
RATE_CODE_ID = [1, 2, 3, 4, 5, 6]
PAYMENT_TYPE = [1, 2, 3, 4, 5, 6]
COLUMNS = ["passenger_count", "trip_distance", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", 
"improvement_surcharge", "total_amount", "congestion_surcharge", "airport_fee", "trip_time", "fare_per_miles"]

def drop_invalid_vendor_ID(df: clean_base.DataFrame):
    """Remove rows with invalid vendor ID"""
    return df.filter(df["VendorID"].isin(VENDOR_ID))

def drop_invalid_rate_code_ID(df: clean_base.DataFrame):
    """Remove rows with invalid rate code ID"""
    return df.filter(df["RatecodeID"].isin(RATE_CODE_ID))

def drop_invalid_rate_payment_type(df: clean_base.DataFrame):
    """Remove rows with invalid payment type"""
    return df.filter(df["payment_type"].isin(PAYMENT_TYPE))

def clean_directory_data(input_directory: str, output_directory: str, taxi_zones: clean_base.DataFrame, 
    spark: clean_base.SparkSession, file_format='parquet'):
    """Cleans all data files within a directory and writes them to an output directory."""
    files = clean_base.list_files_in_directory(input_directory)
    for file in files:
        print(file)
        df = clean_base.read_data(spark, file, file_format)
        print(f"Before: {df.count()}")
        df = clean_base.rename_column(df, ["Airport_fee", "tpep_pickup_datetime", "tpep_dropoff_datetime"], 
            ["airport_fee", "pickup_datetime", "dropoff_datetime"])
        df = df.drop("store_and_fwd_flag")
        # Calculate trip time
        df = df.withColumn("trip_time", (unix_timestamp(col("dropoff_datetime")) - unix_timestamp(col("pickup_datetime"))) / 60)
        # Calculate fare per miles
        df = df.withColumn("fare_per_miles", col("total_amount") / col("trip_distance"))
        df = clean_base.drop_null_values(df)
        print(f"After_NULL: {df.count()}")
        df = clean_base.drop_invalid_locationID(df, taxi_zones)
        start_time, end_time = clean_base.find_valid_time_range(file)
        df = clean_base.drop_invalid_time(df, start_time, end_time)
        df = clean_base.drop_invalid_negative_value(df, COLUMNS)
        df = clean_base.drop_invalid_non_positive_value(df, ["passenger_count", "trip_distance", "trip_time"])
        df = drop_invalid_vendor_ID(df)
        df = drop_invalid_rate_code_ID(df)
        df = drop_invalid_rate_payment_type(df)
        df = clean_base.drop_abnormal_high_values(df, "trip_distance", 500)
        df = clean_base.drop_abnormal_high_values(df, "trip_time", 750)
        [outliers, lower_bound, upper_bound] = detect_outliers(df, "fare_per_miles", 0.1, 0.9)
        print(f"Outliers: {outliers.count()}")
        df = clean_base.drop_outliers(df, "fare_per_miles", lower_bound, upper_bound)
        print(f"After: {df.count()}")
        df.groupBy("VendorID").count().show()
        output_path = clean_base.os.path.join(output_directory, clean_base.os.path.basename(file))
        clean_base.write_data(df, output_path, file_format)