"""Gold: reader-facing aggregates, ready for dashboards, Genie and Delta Sharing."""

from pyspark import pipelines as dp

from utilities.transforms import (
    daily_article_views,
    hourly_traffic,
    quality_summary,
    top_articles,
    trending_articles,
)


@dp.materialized_view(
    name="gold_daily_article_views",
    comment="Views per article per wiki per day, split into desktop and mobile. Articles only.",
    table_properties={"quality": "gold"},
    cluster_by=["view_date", "language"],
)
def gold_daily_article_views():
    return daily_article_views(spark.read.table("silver_pageviews"))


@dp.materialized_view(
    name="gold_daily_top_articles",
    comment="Top 100 most viewed articles per wiki per day.",
    table_properties={"quality": "gold"},
)
def gold_daily_top_articles():
    return top_articles(spark.read.table("gold_daily_article_views"))


@dp.materialized_view(
    name="gold_trending_articles",
    comment="Articles with 5x or more their trailing 7-day average views (min 1,000 views that day).",
    table_properties={"quality": "gold"},
)
def gold_trending_articles():
    return trending_articles(spark.read.table("gold_daily_article_views"))


@dp.materialized_view(
    name="gold_hourly_traffic",
    comment="Total views, mobile share and approximate distinct pages per wiki per hour.",
    table_properties={"quality": "gold"},
)
def gold_hourly_traffic():
    return hourly_traffic(spark.read.table("silver_pageviews"), spark.read.table("silver_wikis"))


@dp.materialized_view(
    name="gold_data_quality",
    comment="Per day: dump lines kept vs quarantined, and failures per quality rule.",
    table_properties={"quality": "gold"},
)
def gold_data_quality():
    return quality_summary(
        spark.read.table("silver_pageviews"), spark.read.table("silver_pageviews_quarantine")
    )
