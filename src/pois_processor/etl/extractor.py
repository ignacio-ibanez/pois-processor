import re

from pyspark.sql.functions import udf, col, split, explode, regexp_extract, concat_ws, trim, translate
from pyspark.sql.types import ArrayType, StringType
import geohash
from geolib import geohash as geo

from src.pois_processor.schemas.poi import poi_schema


class Extractor:

    def __init__(self, data_path):
        self.data_path = data_path
        self.spark = SparkSession.builder \
            .master("local[2]") \
            .appName("pois_processor_local") \
            .getOrCreate()

    def extract_data(self):
        # UDFs
        get_wkt_long = udf(lambda wkt_point: float(wkt_point.split(" ")[1][1:]))
        get_wkt_lat = udf(lambda wkt_point: float(wkt_point.split(" ")[2][:-1]))
        get_geohash = udf(
            lambda long_col, lat_col: geohash.encode(longitude=float(long_col), latitude=float(lat_col), precision=7)
        )

        get_point_long = udf(lambda point: float(point[0]))
        get_point_lat = udf(lambda point: float(point[1]))

        generate_neighbours = udf(lambda ref_geohash: list(geo.neighbours(ref_geohash)) + [ref_geohash])

        # Data
        pois_df = self.spark.read.schema(poi_schema).json(self.data_path)

        # -------
        # Stage 1 and 3
        # -------
        coordinates_df = pois_df \
            .filter(pois_df.polygon.isNull()) \
            .withColumn("long", get_wkt_long(pois_df.coordinate)) \
            .withColumn("lat", get_wkt_lat(pois_df.coordinate))

        geohashes_coordinates_df = coordinates_df \
            .filter(coordinates_df.lat <= 90).filter(coordinates_df.lat > -90) \
            .withColumn("geohash", get_geohash(col("long"), col("lat")))
        """
        geohashes_coordinates_df \
            .select(["venue_id", "geohash"])\
            .groupby("geohash")\
            .count()\
            .orderBy(col("count").desc())#.show()
        """

        regex_list = '\[(.*)\]'
        neighbours_geohashes_coordinates_df = geohashes_coordinates_df \
            .withColumn("geohashes_string", generate_neighbours("geohash")) \
            .withColumn("geohashes_string_cleaned", regexp_extract(col("geohashes_string"), regex_list, 1)) \
            .select("venue_id", split(col("geohashes_string_cleaned"), ",").alias("geohashes")) \
            .select("venue_id", explode("geohashes").alias("geohash_inter")) \
            .select("venue_id", trim(col("geohash_inter")).alias("geohash")) \
            .groupby("geohash") \
            .count() \
            .orderBy(col("count").desc())

        neighbours_geohashes_coordinates_df.show(truncate=False)

        # -------
        # Stage 2
        # -------
        pois_polygons_df = pois_df \
            .filter(pois_df.polygon.isNotNull())

        polygons_df = pois_polygons_df.filter(pois_polygons_df["polygon"].rlike("POLYGON"))
        polygons_points_df = self._get_polygon_points(polygons_df, "POLYGON")

        multipolygons_df = pois_polygons_df.filter(pois_polygons_df["polygon"].rlike("MULTIPOLYGON"))
        multipolygons_points_df = self._get_polygon_points(multipolygons_df, "MULTIPOLYGON")

        line_strings_df = pois_polygons_df.filter(pois_polygons_df["polygon"].rlike("LINESTRING"))
        line_strings_points_df = self._get_polygon_points(line_strings_df, "LINESTRING")

        union_polygons_df = polygons_points_df.union(multipolygons_points_df).union(line_strings_points_df)
        coordinates_polygons_df = union_polygons_df \
            .select(col("*"), translate(col("point"), "()", "").alias("point_cleaned")) \
            .select(col("*"), split(col("point_cleaned"), " ").alias("point_coordinates")) \
            .withColumn("long", get_point_long("point_coordinates")) \
            .withColumn("lat", get_point_lat("point_coordinates"))

        geohashes_coordinates_polygons_df = coordinates_polygons_df \
            .filter(coordinates_polygons_df.lat <= 90).filter(coordinates_polygons_df.lat > -90) \
            .withColumn("geohash", get_geohash(col("long"), col("lat")))

        geohashes_coordinates_polygons_df \
            .select(["venue_id", "geohash"]).groupby("geohash").count().orderBy(col("count").desc()) \
            # .show()

        # polygons_points_list_df.select("polygon_points_list").show(n=100, truncate=False)


    @staticmethod
    def _get_polygon_points(df, wkt_polygon):
        regex_polygon = '\(\((.*)\)\)'
        regex_multipolygon = '\(\(\((.*)\)\)\)'
        regex_linestring = '\((.*)\)'
        if wkt_polygon.startswith("POLYGON"):
            df = df \
                .withColumn("points_str", regexp_extract(col("polygon"), regex_polygon, 1)) \
                .select(col("*"), split(col("points_str"), ",").alias("points_list")) \
                .select(col("*"), explode("points_list").alias("point_"))
            df = df.withColumn("point", trim(col("point_"))) \
                .drop("points_str", "points_list", "point_")
        elif wkt_polygon.startswith("LINESTRING"):
            df = df \
                .withColumn("points_str", regexp_extract(col("polygon"), regex_linestring, 1)) \
                .select(col("*"), split(col("points_str"), ",").alias("points_list")) \
                .select(col("*"), explode("points_list").alias("point_"))
            df = df.withColumn("point", trim(col("point_"))) \
                .drop("points_str", "points_list", "point_")
        elif wkt_polygon.startswith("MULTIPOLYGON"):
            df = df \
                .withColumn("points_str", regexp_extract(col("polygon"), regex_multipolygon, 1)) \
                .select(col("*"), split(col("points_str"), "\)\), \(\(").alias("points_intermediate")) \
                .withColumn("points_", concat_ws(",", col("points_intermediate"))) \
                .select(col("*"), split(col("points_"), ",").alias("points_list")) \
                .select(col("*"), explode("points_list").alias("point_"))
            df = df.withColumn("point", trim(col("point_"))) \
                .drop("points_str", "points_intermediate", "points_", "points_list", "point_")
        else:
            raise(f"Not valid format: {wkt_polygon}")

        return df
