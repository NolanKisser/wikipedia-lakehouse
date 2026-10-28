"""Pure DataFrame transformations for the Wikipedia pageviews pipeline.

Nothing here touches pipeline decorators or tables, so every function can be unit tested
with a local SparkSession (see tests/). The pipeline files in transformations/ only wire
these functions to sources and sinks.

Everything is written to be safe under ANSI SQL mode (the default on Spark 4 and Databricks):
malformed input becomes null via try_* functions instead of failing the whole update.
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

LINEAGE_COLUMNS = ["_source_file", "_ingested_at"]

# Project suffixes in pageview domain codes, e.g. "en.d" = English Wiktionary.
# https://wikitech.wikimedia.org/wiki/Data_Platform/Data_Lake/Traffic/Pageviews
PROJECT_SUFFIXES = {
    "b": "wikibooks",
    "d": "wiktionary",
    "f": "foundation",
    "n": "wikinews",
    "q": "wikiquote",
    "s": "wikisource",
    "v": "wikiversity",
    "voy": "wikivoyage",
    "w": "mediawiki",
    "wd": "wikidata",
    "z": "wikipedia",
}

# For *.wikimedia.org sites the first ".m" is the project suffix, not "mobile":
# "commons.m" is desktop Commons and "commons.m.m" is mobile Commons.
WIKIMEDIA_SITES = ["commons", "meta", "species", "incubator", "outreach", "wikimania", "foundation"]

# Titles that aren't reader-facing articles. English namespace names, so this heuristic is
# exact for English wikis and approximate elsewhere.
NON_ARTICLE_PAGES = ["Main_Page", "-"]
NON_ARTICLE_PREFIXES = [
    "Special",
    "Talk",
    "User",
    "User_talk",
    "Wikipedia",
    "Wikipedia_talk",
    "File",
    "File_talk",
    "MediaWiki",
    "MediaWiki_talk",
    "Template",
    "Template_talk",
    "Help",
    "Help_talk",
    "Category",
    "Category_talk",
    "Portal",
    "Portal_talk",
    "Draft",
    "Draft_talk",
    "Module",
    "Module_talk",
    "TimedText",
    "TimedText_talk",
]

# Silver keeps only rows passing every rule; everything else lands in the quarantine table.
# Each rule null-guards its columns so it is strictly true or false.
QUALITY_RULES = {
    "well_formed": "field_count IS NOT NULL AND field_count = 4",
    "known_hour": "view_hour IS NOT NULL",
    "positive_views": "views IS NOT NULL AND views > 0",
    "has_title": "page_title IS NOT NULL AND page_title NOT IN ('', '-')",
    "valid_domain": "domain_code IS NOT NULL AND domain_code RLIKE '^[a-z0-9-]+([.][a-z]+)*$'",
}


def quarantine_condition() -> str:
    """SQL predicate for rows failing at least one rule: the exact complement of silver."""
    return " OR ".join(f"NOT coalesce(({rule}), false)" for rule in QUALITY_RULES.values())


def parse_pageviews(bronze: DataFrame) -> DataFrame:
    """Split raw dump lines ("domain_code page_title views 0") and take the hour from the file name."""
    # Dumps are named for the UTC hour they cover, e.g. pageviews-20251001-130000.gz.
    hour = F.nullif(F.regexp_extract("_source_file", r"pageviews-(\d{8}-\d{2})\d{4}\.gz$", 1), F.lit(""))
    return (
        bronze.withColumn("_fields", F.split("value", " "))
        .select(
            F.expr("try_element_at(_fields, 1)").alias("domain_code"),
            F.expr("try_element_at(_fields, 2)").alias("page_title"),
            F.expr("try_cast(try_element_at(_fields, 3) AS BIGINT)").alias("views"),
            F.size("_fields").alias("field_count"),
            F.try_to_timestamp(F.concat(hour, F.lit("Z")), F.lit("yyyyMMdd-HHX")).alias("view_hour"),
            *[F.col(c) for c in LINEAGE_COLUMNS if c in bronze.columns],
        )
        .withColumn("view_date", F.to_date("view_hour"))
    )


def classify_domain(df: DataFrame) -> DataFrame:
    """Derive language, project and access (desktop/mobile) from domain_code."""
    parts = F.split("domain_code", r"\.")
    language = F.try_element_at(parts, F.lit(1))
    is_wikimedia = language.isin(WIKIMEDIA_SITES)
    rest = F.slice(parts, 2, 10)
    rest = F.when(is_wikimedia & (F.try_element_at(rest, F.lit(1)) == "m"), F.slice(rest, 2, 10)).otherwise(
        rest
    )
    suffix = F.try_element_at(F.filter(rest, lambda part: part != "m"), F.lit(-1))
    suffix_lookup = F.create_map(*[F.lit(v) for kv in PROJECT_SUFFIXES.items() for v in kv])
    return (
        df.withColumn("language", language)
        .withColumn(
            "project",
            F.when(is_wikimedia, F.lit("wikimedia"))
            .when(suffix.isNull(), F.lit("wikipedia"))
            .otherwise(F.coalesce(F.try_element_at(suffix_lookup, suffix), F.lit("other"))),
        )
        .withColumn("access", F.when(F.array_contains(rest, "m"), "mobile").otherwise("desktop"))
    )


def clean_pageviews(bronze: DataFrame) -> DataFrame:
    """Parse and classify raw lines and flag reader-facing articles."""
    prefixes = "|".join(NON_ARTICLE_PREFIXES)
    return (
        classify_domain(parse_pageviews(bronze))
        .withColumn("article", F.regexp_replace("page_title", "_", " "))
        .withColumn(
            "is_content",
            F.coalesce(
                ~F.col("page_title").isin(NON_ARTICLE_PAGES) & ~F.col("page_title").rlike(f"^({prefixes}):"),
                F.lit(False),
            ),
        )
    )


def clean_wikis(raw: DataFrame) -> DataFrame:
    """Select the wiki site list columns, one row per join key so lookups never fan out."""
    return raw.select("language_code", "language_name", "project", "site_url", "dbname").dropDuplicates(
        ["language_code", "project"]
    )


def daily_article_views(pageviews: DataFrame) -> DataFrame:
    """Views per article per day, split by desktop and mobile. Articles only."""
    return (
        pageviews.where("is_content")
        .groupBy("view_date", "language", "project", "page_title", "article")
        .agg(
            F.sum(F.when(F.col("access") == "desktop", F.col("views")).otherwise(0)).alias("desktop_views"),
            F.sum(F.when(F.col("access") == "mobile", F.col("views")).otherwise(0)).alias("mobile_views"),
            F.sum("views").alias("views"),
        )
    )


def top_articles(daily: DataFrame, n: int = 100) -> DataFrame:
    """The n most viewed articles per wiki per day."""
    ranking = Window.partitionBy("view_date", "language", "project").orderBy(F.desc("views"), "page_title")
    return daily.withColumn("rank", F.row_number().over(ranking)).where(F.col("rank") <= n)


def trending_articles(
    daily: DataFrame, min_views: int = 1000, min_ratio: float = 5.0, baseline_days: int = 7
) -> DataFrame:
    """Articles whose views jumped versus their average over the previous baseline_days.

    days_of_history is how many of those days the loaded data actually covers, and the baseline
    averages over exactly those days. Days the article had no views count as zero, so the
    baseline is a true daily average. The first day loaded has no history, and nothing on it
    is reported as trending.
    """
    day_number = F.datediff("view_date", F.to_date(F.lit("1970-01-01")))
    history = (
        Window.partitionBy("language", "project", "page_title")
        .orderBy("_day")
        .rangeBetween(-baseline_days, -1)
    )
    first_day = daily.agg(F.min("view_date").alias("_first_day"))
    days_of_history = F.least(F.lit(baseline_days), F.datediff("view_date", "_first_day"))
    baseline = F.coalesce(F.sum("views").over(history), F.lit(0)) / F.col("days_of_history")
    return (
        daily.withColumn("_day", day_number)
        .crossJoin(first_day)
        .withColumn("days_of_history", days_of_history)
        .withColumn("baseline_views", F.when(F.col("days_of_history") > 0, F.round(baseline, 1)))
        .withColumn(
            "spike_ratio",
            F.when(F.col("baseline_views") > 0, F.round(F.col("views") / F.col("baseline_views"), 1)),
        )
        .where(
            (F.col("views") >= min_views)
            & (F.col("days_of_history") > 0)
            & ((F.col("spike_ratio") >= min_ratio) | (F.col("baseline_views") == 0))
        )
        .select(
            "view_date",
            "language",
            "project",
            "page_title",
            "article",
            "views",
            "baseline_views",
            "spike_ratio",
            "days_of_history",
        )
    )


def hourly_traffic(pageviews: DataFrame, wikis: DataFrame) -> DataFrame:
    """Total views, mobile share and distinct pages per wiki per hour, all pages included."""
    sites = wikis.select(F.col("language_code").alias("language"), "project", "language_name", "site_url")
    return (
        pageviews.groupBy("view_hour", "language", "project")
        .agg(
            F.sum("views").alias("views"),
            F.sum(F.when(F.col("access") == "mobile", F.col("views")).otherwise(0)).alias("mobile_views"),
            F.approx_count_distinct("page_title").alias("approx_distinct_pages"),
        )
        .withColumn("mobile_share", F.round(F.col("mobile_views") / F.col("views"), 4))
        .join(sites, ["language", "project"], "left")
    )


def quality_summary(passed: DataFrame, quarantined: DataFrame, key: str = "view_date") -> DataFrame:
    """Per key: rows kept, rows quarantined, and how many failed each rule."""
    kept = passed.groupBy(key).agg(F.count(F.lit(1)).alias("passed"))
    failed = quarantined.groupBy(key).agg(
        F.count(F.lit(1)).alias("quarantined"),
        *[
            F.sum(F.when(F.expr(f"coalesce(({rule}), false)"), 0).otherwise(1)).alias(f"failed_{name}")
            for name, rule in QUALITY_RULES.items()
        ],
    )
    count_columns = ["passed", "quarantined", *(f"failed_{name}" for name in QUALITY_RULES)]
    return (
        kept.join(failed, key, "full_outer")
        .fillna(0, subset=count_columns)
        .withColumn(
            "quarantine_pct",
            F.round(F.col("quarantined") / (F.col("passed") + F.col("quarantined")) * 100, 3),
        )
    )
