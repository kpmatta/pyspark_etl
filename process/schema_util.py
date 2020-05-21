from pyspark.sql.types import StructType
from pyspark.sql import SparkSession
import json


# local file reading
def read_file_to_string(path):
    f = open(path, 'r')
    contents = f.read()
    f.close()
    return contents


# Works in hdfs also
def read_content(path):
    spark = SparkSession.builder.getOrCreate()
    content_arr = spark.sparkContext.textFile(path, 1).collect()
    content = "\n".join(content_arr)
    return content


# convert json file content into schema StructType
def read_schema_from_file(path):
    schema_str = read_content(path)
    return StructType.fromJson(json.loads(schema_str))


# Create a SQL select expression from the Schema metadata
# "function" : Spark SQL function
# "alias"    : Rename the column
def get_functions_from_schema(schema: StructType):
    select_names = []
    for f in schema:
        function = f.name
        if 'alias' in f.metadata:
            function = f.metadata['alias']

        if 'function' in f.metadata:
            function = f.metadata['function'] + ' as ' + function

        select_names.append(function)

    return select_names
