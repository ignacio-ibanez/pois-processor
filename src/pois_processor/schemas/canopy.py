from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

canopy_schema = StructType([
    StructField("geohash", StringType(), False),
    StructField("count", IntegerType(), False),
    StructField("venues", ArrayType(StructType([
        StructField("id", StringType(), False),
        StructField("categories", StringType(), False)
    ])), False)
])
