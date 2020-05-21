from process import read_file, write_file


# read csv from the input path and transform by schema metadata, writes to output path
def csv_to_parquet(input_file_path : str, schema_path : str, output_path:str):
    in_df = read_file.read_csv(input_file_path, schema_path)
    write_file.write_parquet(in_df, output_path)
    print("Parquet created at ["+ output_path + "]")