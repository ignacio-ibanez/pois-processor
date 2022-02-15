from pyspark.sql.functions import udf, col, split, explode, regexp_extract, trim, translate
import geohash
from geolib import geohash as geo

from src.pois_processor.etl.transform_utils import get_polygons_linestring_points, get_multipolygon_points


class Processor:

    def __init__(self, spark):
        self.spark = spark

    def process_data(self, data_path, data_schema):
        transformed_fields = ["venue_id", "categories", "geohash"]

        pois_df = self.spark.read.schema(data_schema).json(data_path)

        # Transform POIs without polygon (based on a single coordinate)
        coordinates_df = pois_df.filter(pois_df.polygon.isNull())
        geohashes_coordinates_df = self._get_geohashes_coordinates(coordinates_df).select(transformed_fields)

        # Transform POIs with polygon, multipolygon or linestring
        polygons_df = pois_df.filter(pois_df.polygon.isNotNull())
        geohashes_polygons_df = self._get_geohashes_polygons(polygons_df).select(transformed_fields)

        # Union both dfs from coordinates and polygons, and group by geohash
        pois_transformed_df = geohashes_coordinates_df.union(geohashes_polygons_df) \
            .groupby("geohash") \
            .count() \
            .orderBy(col("count").desc())

        return pois_transformed_df

    @staticmethod
    def _get_geohashes_coordinates(coordinates_df):
        # UDFs
        get_wkt_long = udf(lambda wkt_point: float(wkt_point.split(" ")[1][1:]))
        get_wkt_lat = udf(lambda wkt_point: float(wkt_point.split(" ")[2][:-1]))
        get_geohash = udf(
            lambda long_col, lat_col: geohash.encode(longitude=float(long_col), latitude=float(lat_col), precision=7)
        )
        generate_neighbours = udf(lambda ref_geohash: list(geo.neighbours(ref_geohash)) + [ref_geohash])

        geohashes_df = coordinates_df \
            .withColumn("long", get_wkt_long(col("coordinate"))) \
            .withColumn("lat", get_wkt_lat(col("coordinate"))) \
            .filter(col("lat") <= 90) \
            .filter(col("lat") > -90) \
            .withColumn("geohash", get_geohash(col("long"), col("lat")))

        regex_list = '\[(.*)\]'
        neighbours_geohashes_coordinates_df = geohashes_df \
            .withColumn("geohashes_string", generate_neighbours("geohash")) \
            .withColumn("geohashes_string_cleaned", regexp_extract(col("geohashes_string"), regex_list, 1)) \
            .drop("geohash") \
            .select(col("*"), split(col("geohashes_string_cleaned"), ",").alias("geohashes")) \
            .select(col("*"), explode("geohashes").alias("geohash_inter")) \
            .select(col("*"), trim(col("geohash_inter")).alias("geohash"))

        return neighbours_geohashes_coordinates_df

    @staticmethod
    def _get_geohashes_polygons(multipoint_df):
        # UDFs
        get_point_long = udf(lambda point: float(point[0]))
        get_point_lat = udf(lambda point: float(point[1]))
        get_geohash = udf(
            lambda long_col, lat_col: geohash.encode(longitude=float(long_col), latitude=float(lat_col), precision=7)
        )

        polygons_points_df = get_polygons_linestring_points(
            multipoint_df.filter(multipoint_df["polygon"].rlike("POLYGON")), "polygon"
        )
        multipolygons_points_df = get_multipolygon_points(
            multipoint_df.filter(multipoint_df["polygon"].rlike("MULTIPOLYGON"))
        )
        line_strings_points_df = get_polygons_linestring_points(
            multipoint_df.filter(multipoint_df["polygon"].rlike("LINESTRING")), "linestring"
        )
        polygons_points_df = polygons_points_df.union(multipolygons_points_df).union(line_strings_points_df)

        coordinates_polygons_df = polygons_points_df \
            .select(col("*"), translate(col("point"), "()", "").alias("point_cleaned")) \
            .select(col("*"), split(col("point_cleaned"), " ").alias("point_coordinates")) \
            .withColumn("long", get_point_long("point_coordinates")) \
            .withColumn("lat", get_point_lat("point_coordinates"))

        geohashes_polygons_df = coordinates_polygons_df \
            .filter(coordinates_polygons_df.lat <= 90) \
            .filter(coordinates_polygons_df.lat > -90) \
            .withColumn("geohash", get_geohash(col("long"), col("lat")))

        return geohashes_polygons_df
