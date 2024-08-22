from . import clean_base

VENDOR_ID = [1, 2]
RATE_CODE_ID = [1, 2, 3, 4, 5, 6]
PAYMENT_TYPE = [1, 2, 3, 4, 5, 6]

def drop_invalid_vendor_ID(df: clean_base.DataFrame):
    """Remove rows with invalid vendor ID"""
    return df.filter(df["VendorID"].isin(VENDOR_ID))

def drop_invalid_rate_code_ID(df: clean_base.DataFrame):
    """Remove rows with invalid Rate Code ID"""
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
        df = clean_base.drop_null_values(df)
        df = clean_base.drop_invalid_locationID(df, taxi_zones)
        start_time, end_time = clean_base.find_valid_time_range(file)
        df = clean_base.drop_invalid_time(df, start_time, end_time)
        df = clean_base.drop_invalid_non_positive_value(df, "passenger_count")
        df = clean_base.drop_invalid_non_positive_value(df, "trip_distance")
        df = drop_invalid_vendor_ID(df)
        df = drop_invalid_rate_code_ID(df)
        df = drop_invalid_rate_payment_type(df)
        print(f"After: {df.count()}")
        output_path = clean_base.os.path.join(output_directory, clean_base.os.path.basename(file))
        clean_base.write_data(df, output_path, file_format)