from pyspark.sql import SparkSession

from src.pois_processor.etl.processor import Processor
from src.pois_processor.schemas.poi import poi_schema


data_path = "../../data/90per"
output_path = "../../data/output"


def main():
    spark = SparkSession.builder \
            .master("local[2]") \
            .appName("pois_processor_local") \
            .getOrCreate()

    processor = Processor(spark=spark)

    transformed_df = processor.process_data(data_path=data_path, data_schema=poi_schema)

    transformed_df \
        .limit(1000) \
        .repartition(1).write.json(output_path, mode="overwrite")


if __name__ == "__main__":
    main()
