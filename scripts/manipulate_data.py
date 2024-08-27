from pyspark.sql import DataFrame
from pyspark.sql.functions import min, max, lit, col, avg

def group_by_and_avg(df: DataFrame, group_col: str, value_cols: list[str]):
    """
    Groups the DataFrame by the specified column and calculates the average of other columns.
    """
    # Create aggregation expressions for the average of each value column
    agg_exprs = [avg(col).alias(f"average_{col}") for col in value_cols]
    
    # Perform the group by and aggregation
    result_df = df.groupBy(group_col).agg(*agg_exprs)
    
    return result_df.toPandas()

def detect_outliers(df: DataFrame, column: str, cq1: float, cq3: float):
    """Dectect outliers"""
    quantiles = df.stat.approxQuantile(column, [cq1, cq3], 0.05)
    Q1 = quantiles[0]
    Q3 = quantiles[1]
    IQR = Q3 - Q1

    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR

    print(f"lower: {lower_bound}, upper: {upper_bound}")

    outliers = df.filter((col(column) < lower_bound) | (col(column) > upper_bound))
    return [outliers, lower_bound, upper_bound]

def find_min_max_df(df: DataFrame, columns: list[str]):
    """
    Computes the minimum and maximum values for specified columns in a PySpark DataFrame
    and returns the results as a DataFrame.
    
    Parameters:
        df (DataFrame): The PySpark DataFrame to analyze.
        columns (list): A list of column names to compute statistics for.
    
    Returns:
        DataFrame: A DataFrame with columns for the metric, minimum, and maximum values.
    """
    # Initialize an empty DataFrame to store results
    results_df = None
    
    # Iterate over each column and calculate min and max
    for column in columns:
        # Ensure column is in the DataFrame
        if column in df.columns:
            # Aggregate the minimum and maximum values for the column
            agg_df = df.agg(
                min(column).alias("Min"),
                max(column).alias("Max")
            ).withColumn("Column", lit(column))
            
            # Rearrange columns for consistency
            agg_df = agg_df.select("Column", "Min", "Max")
            
            # Union with the results DataFrame
            if results_df is None:
                results_df = agg_df
            else:
                results_df = results_df.union(agg_df)
    
    return results_df
