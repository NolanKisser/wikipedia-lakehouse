"""Silver: parsed, classified pageviews with enforced quality rules, plus a quarantine for rejects."""

from pyspark import pipelines as dp

from utilities.transforms import QUALITY_RULES, clean_pageviews, clean_wikis, quarantine_condition


@dp.table(
    name="silver_pageviews",
    comment="Hourly views per page with language, project and access. Rows failing QUALITY_RULES dropped.",
    table_properties={"quality": "silver"},
    cluster_by=["view_date", "language"],
)
@dp.expect_all_or_drop(QUALITY_RULES)
@dp.expect("plausible_hourly_views", "views < 5000000")  # tracked, not enforced: flags bot spikes
def silver_pageviews():
    return clean_pageviews(spark.readStream.table("bronze_pageviews"))


@dp.table(
    name="silver_pageviews_quarantine",
    comment="Dump lines that failed at least one silver quality rule, kept for inspection.",
    table_properties={"quality": "quarantine"},
)
def silver_pageviews_quarantine():
    return clean_pageviews(spark.readStream.table("bronze_pageviews")).where(quarantine_condition())


@dp.materialized_view(
    name="silver_wikis",
    comment="Wiki dimension: (language_code, project) -> language name, site URL, dbname.",
    table_properties={"quality": "silver"},
)
@dp.expect_or_fail("wiki_key_present", "language_code IS NOT NULL AND project IS NOT NULL")
def silver_wikis():
    return clean_wikis(spark.read.table("bronze_wikis"))
