from distutils.command import clean
from . import clean_base

UBER = "HV0003"

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
        df = drop_non_uber(df)
        df = df.drop(*["hvfhs_license_num", "originating_base_num", "shared_request_flag", "shared_match_flag", "access_a_ride_flag", "wav_request_flag", "wav_match_flag"])
        df = clean_base.drop_null_values(df)
        df = clean_base.drop_invalid_locationID(df, taxi_zones)
        start_time, end_time = clean_base.find_valid_time_range(file)
        df = clean_base.drop_invalid_time(df, start_time, end_time)
        df = clean_base.drop_invalid_non_positive_value(df, "trip_distance")
        df = clean_base.drop_invalid_non_positive_value(df, "trip_time")
        df = clean_base.sampling_data(df, "dispatching_base_num", 0.05)
        print(f"After: {df.count()}")
        output_path = clean_base.os.path.join(output_directory, clean_base.os.path.basename(file))
        clean_base.write_data(df, output_path, file_format)

