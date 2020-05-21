from process import read_file, write_file
import sys


# read csv from the input path and transform by schema metadata, writes to output path
def csv_to_parquet(input_file_path : str, schema_path : str, output_path:str):
    in_df = read_file.read_csv(input_file_path, schema_path)
    write_file.write_parquet(in_df, output_path)


# args
# 1. input csv file path
# 2. input schema file path
# 3. output parquet path
if __name__ == '__main__':
    if len(sys.argv) != 4:
        raise Exception("Invalid arguments passed")

    csv_to_parquet(sys.argv[1], sys.argv[2], sys.argv[3])