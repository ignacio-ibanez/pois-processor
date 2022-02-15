from pyspark.sql import SparkSession

from src.pois_processor.etl.transform_utils import

def main():

    spark = SparkSession.builder \
            .master("local[2]") \
            .appName("pois_processor_local") \
            .getOrCreate()



    df_event = read_file(input_path=config["input_path"], spark=spark)
    df_event = flatten_df_struct_columns_to_root(nested_df=df_event)
    df_event = validate_dataframe_columns_data_type(df=df_event, expected_schema=EXPECTED_SCHEMA)
    df_event = filter_invalid_records(df_event)

    write_file(
        df=select_final_columns(df_event),
        output_path=config["output_path"]
    )


if __name__ == "__main__":
    main()
