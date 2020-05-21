from process import convert
import sys

# args
# 1. input csv file path
# 2. input schema file path
# 3. output parquet path
if __name__ == '__main__':
    if len(sys.argv) != 4:
        raise Exception("Invalid arguments passed")

    convert.csv_to_parquet(sys.argv[1], sys.argv[2], sys.argv[3])