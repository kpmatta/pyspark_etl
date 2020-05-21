from pyspark.sql import SparkSession, DataFrame


# Write DataFrame to output path
def write_parquet(out_df: DataFrame, out_path : str):
    out_df.write.mode("overwrite").parquet(out_path)
