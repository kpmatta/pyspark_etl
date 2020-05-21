from pyspark.sql import SparkSession
from process import schema_util
import sys


#  process the csv file with the schema and apply sql functions from schema metadata
#  return transformed dataframe
def read_csv(file_path, schema_path):
    spark = SparkSession.builder.getOrCreate()
    schema = schema_util.read_schema_from_file(schema_path)
    select_flds = schema_util.get_functions_from_schema(schema)

    df = ( spark.read
           .option('header', True)
            .schema(schema)
           .csv(file_path))

    return df.selectExpr(select_flds)
