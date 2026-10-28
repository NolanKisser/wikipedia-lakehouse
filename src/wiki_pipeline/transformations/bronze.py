"""Bronze: raw files from the landing volume, loaded as-is plus lineage columns."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

RAW_PATH = spark.conf.get("wiki.raw_path")


@dp.table(
    name="bronze_pageviews",
    comment="Raw lines from Wikimedia hourly pageview dumps, loaded incrementally with Auto Loader.",
    table_properties={"quality": "bronze"},
)
def bronze_pageviews():
    # Gzip is decompressed transparently. Auto Loader tracks which files it has ingested, so
    # each run only reads hours that are new.
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "text")
        .load(f"{RAW_PATH}/pageviews/")
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
    )


@dp.materialized_view(
    name="bronze_wikis",
    comment="Raw wiki site list (language x project) flattened from the Wikimedia sitematrix API.",
    table_properties={"quality": "bronze"},
)
def bronze_wikis():
    return spark.read.option("header", True).csv(f"{RAW_PATH}/wikis/")
