from pyspark.sql.functions import col, split, explode, regexp_extract, concat_ws, trim


def get_polygons_linestring_points(df, polygon_type):
    if polygon_type == "polygon":
        regex = "\(\((.*)\)\)"
    elif polygon_type == "linestring":
        regex = "\((.*)\)"
    else:
        raise (f"Not valid format: {polygon_type}")

    df = df \
        .withColumn("points_str", regexp_extract(col("polygon"), regex, 1)) \
        .select(col("*"), split(col("points_str"), ",").alias("points_list")) \
        .select(col("*"), explode("points_list").alias("point_"))
    df = df.withColumn("point", trim(col("point_"))) \
        .drop("points_str", "points_list", "point_")

    return df


def get_multipolygon_points(df):
    regex_multipolygon = "\(\(\((.*)\)\)\)"
    df = df \
        .withColumn("points_str", regexp_extract(col("polygon"), regex_multipolygon, 1)) \
        .select(col("*"), split(col("points_str"), "\)\), \(\(").alias("points_intermediate")) \
        .withColumn("points_", concat_ws(",", col("points_intermediate"))) \
        .select(col("*"), split(col("points_"), ",").alias("points_list")) \
        .select(col("*"), explode("points_list").alias("point_"))
    df = df.withColumn("point", trim(col("point_"))) \
        .drop("points_str", "points_intermediate", "points_", "points_list", "point_")

    return df
