from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType, ArrayType

poi_schema = StructType([
    StructField("venue_id", StringType(), False),
    StructField("area", FloatType(), False),
    StructField("polygon", StringType(), True),
    StructField("coordinate", StringType(), False),
    StructField("categories", ArrayType(StructType([
        StructField("category_id", StringType(), True),
        StructField("name", StringType(), True)
    ])), True),
    StructField("update_time", TimestampType(), False)
])
