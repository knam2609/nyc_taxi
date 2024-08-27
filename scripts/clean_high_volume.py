from . import clean_base
from .manipulate_data import detect_outliers
from pyspark.sql.functions import col, unix_timestamp

UBER = "HV0003"
COLUMNS = ["trip_distance", "trip_time", "base_passenger_fare", "tolls", "bcf",
 "sales_tax", "congestion_surcharge", "airport_fee", "tips", "driver_pay", "total_amount", "waiting_time", "fare_per_miles"]

def drop_non_uber(df: clean_base.DataFrame):
    """Remove rows which is not Uber"""
    return df.filter(df["hvfhs_license_num"] == UBER)

def clean_directory_data(input_directory: str, output_directory: str, taxi_zones: clean_base.DataFrame, 
    spark: clean_base.SparkSession, file_format='parquet'):
    """Cleans all data files within a directory and writes them to an output directory."""
    files = clean_base.list_files_in_directory(input_directory)
    for file in files:
        print(file)
        df = clean_base.read_data(spark, file, file_format)
        print(f"Before: {df.count()}")
        df = clean_base.rename_column(df, ["trip_miles"], ["trip_distance"])
        df = df.withColumn("total_amount", (col("base_passenger_fare") + col("tolls") + col("bcf") + col("sales_tax") + col("congestion_surcharge") + col("airport_fee") + col("tips")))
        # Calculate the waiting time in minutes
        df = df.withColumn("waiting_time", (unix_timestamp(col("on_scene_datetime")) - unix_timestamp(col("request_datetime"))) / 60)
        # Turn trip_time to minutes
        df = df.withColumn("trip_time", (col("trip_time") / 60))
        # Calculate fare per miles
        df = df.withColumn("fare_per_miles", (col("total_amount") / col("trip_distance")))
        df = drop_non_uber(df)
        df = clean_base.drop_null_values(df)
        print(f"After_NULL: {df.count()}")
        df = df.drop(*["hvfhs_license_num", "originating_base_num", "shared_request_flag", "shared_match_flag", "access_a_ride_flag", "wav_request_flag", "wav_match_flag"])
        df = clean_base.drop_invalid_locationID(df, taxi_zones)
        start_time, end_time = clean_base.find_valid_time_range(file)
        df = clean_base.drop_invalid_time(df, start_time, end_time)
        df = clean_base.drop_invalid_negative_value(df, COLUMNS)
        df = clean_base.drop_invalid_non_positive_value(df, ["trip_distance", "trip_time"])
        df = clean_base.drop_abnormal_high_values(df, "trip_distance", 500)
        df = clean_base.drop_abnormal_high_values(df, "trip_time", 750)
        [outliers, lower_bound, upper_bound] = detect_outliers(df, "fare_per_miles", 0.1, 0.9)
        print(f"Outliers: {outliers.count()}")
        df = clean_base.drop_outliers(df, "fare_per_miles", lower_bound, upper_bound)
        print(f"After: {df.count()}")
        df.groupBy("dispatching_base_num").count().show()
        output_path = clean_base.os.path.join(output_directory, clean_base.os.path.basename(file))
        clean_base.write_data(df, output_path, file_format)

