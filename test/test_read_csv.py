from unittest import TestCase
import os
from process import read_file, convert
from pyspark.sql import SparkSession


class Test(TestCase):
    def test_read_file(self):
        spark = SparkSession.builder.master('local[*]').appName('test').getOrCreate()

        path = os.path.dirname(__file__)
        path_arr = os.path.split(path)
        file_path = path_arr[0] + '/test_files/sample.csv'
        schema_path = path_arr[0] + '/test_files/schema.json'
        df = read_file.read_csv(file_path, schema_path)
        df.printSchema()
        df.show()

    def test_csv_to_parquet(self):
        spark = SparkSession.builder.master('local[*]').appName('test').getOrCreate()
        path = os.path.dirname(__file__)
        path_arr = os.path.split(path)
        input_path = path_arr[0] + '/test_files/sample.csv'
        schema_path = path_arr[0] + '/test_files/schema.json'
        output_path = path_arr[0] + '/test_files/output'
        convert.csv_to_parquet(input_path, schema_path, output_path)

