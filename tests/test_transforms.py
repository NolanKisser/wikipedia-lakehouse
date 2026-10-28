from datetime import date, timedelta

import pytest
from pyspark.sql import functions as F

from utilities.transforms import (
    QUALITY_RULES,
    classify_domain,
    clean_pageviews,
    daily_article_views,
    hourly_traffic,
    quality_summary,
    quarantine_condition,
    top_articles,
    trending_articles,
)

SOURCE = "/Volumes/workspace/wikipedia/raw/pageviews/pageviews-20251001-130000.gz"
DAILY_SCHEMA = (
    "view_date date, language string, project string, page_title string, article string, views bigint"
)


def bronze(spark, *rows):
    """Bronze rows from dump lines; pass (line, source_file) to override the default file."""
    rows = [row if isinstance(row, tuple) else (row, SOURCE) for row in rows]
    return spark.createDataFrame(rows, "value string, _source_file string")


def split(df):
    """Mimic the pipeline: expect_all_or_drop keeps rows passing every rule."""
    passed = df.where(" AND ".join(f"({rule})" for rule in QUALITY_RULES.values()))
    return passed, df.where(quarantine_condition())


def test_parse_line_and_hour_from_file_name(spark):
    row = (
        clean_pageviews(bronze(spark, "en Taylor_Swift 48213 0"))
        .withColumn("hour", F.date_format("view_hour", "yyyy-MM-dd HH:mm"))
        .first()
    )

    assert (row.domain_code, row.page_title, row.views, row.field_count) == ("en", "Taylor_Swift", 48213, 4)
    assert row.hour == "2025-10-01 13:00"
    assert row.view_date == date(2025, 10, 1)
    assert row.article == "Taylor Swift"
    assert row._source_file == SOURCE


@pytest.mark.parametrize(
    "domain_code, expected",
    [
        ("en", ("en", "wikipedia", "desktop")),
        ("en.m", ("en", "wikipedia", "mobile")),
        ("en.d", ("en", "wiktionary", "desktop")),
        ("de.m.voy", ("de", "wikivoyage", "mobile")),
        ("commons.m", ("commons", "wikimedia", "desktop")),
        ("commons.m.m", ("commons", "wikimedia", "mobile")),
        ("www.wd", ("www", "wikidata", "desktop")),
        ("zh-min-nan", ("zh-min-nan", "wikipedia", "desktop")),
        ("xx.zzz", ("xx", "other", "desktop")),
    ],
)
def test_classify_domain(spark, domain_code, expected):
    row = classify_domain(spark.createDataFrame([(domain_code,)], "domain_code string")).first()

    assert (row.language, row.project, row.access) == expected


@pytest.mark.parametrize(
    "title, is_content",
    [
        ("Taylor_Swift", True),
        ("Alien:_Romulus", True),  # a colon alone doesn't make it a namespace
        ("Main_Page", False),
        ("Special:Search", False),
        ("User_talk:Jimbo_Wales", False),
        ("Wikipedia:Hauptseite", False),
    ],
)
def test_is_content(spark, title, is_content):
    assert clean_pageviews(bronze(spark, f"en {title} 10 0")).first().is_content is is_content


@pytest.mark.parametrize(
    "bad_row, failed_rule",
    [
        ("en Foo 5", "well_formed"),
        ("en Foo 5 0 extra", "well_formed"),
        ("", "well_formed"),
        ("en Foo 0 0", "positive_views"),
        ("en Foo abc 0", "positive_views"),
        ("en - 12 0", "has_title"),
        ("EN! Foo 3 0", "valid_domain"),
        (("en Foo 3 0", "/Volumes/x/raw/pageviews/unexpected.gz"), "known_hour"),
    ],
)
def test_bad_rows_are_quarantined(spark, bad_row, failed_rule):
    passed, quarantined = split(clean_pageviews(bronze(spark, "en Foo 3 0", bad_row)))

    assert passed.count() == 1
    assert quarantined.count() == 1
    assert not quarantined.where(F.expr(f"coalesce(({QUALITY_RULES[failed_rule]}), false)")).count()


def test_silver_and_quarantine_partition_every_row(spark):
    df = clean_pageviews(bronze(spark, "en A 1 0", "en.m A 2 0", "en B", "de - 4 0", "en C x 0", "fr D 0 0"))
    passed, quarantined = split(df)

    assert passed.count() + quarantined.count() == df.count()
    assert passed.count() == 2


def test_daily_views_and_top_articles(spark):
    pageviews = clean_pageviews(
        bronze(
            spark,
            "en A 10 0",
            "en.m A 5 0",
            "en B 20 0",
            "en C 1 0",
            "en Main_Page 999 0",
            "de X 3 0",
            ("en A 7 0", SOURCE.replace("-130000", "-140000")),  # same day, next hour
        )
    )
    daily = {(r.language, r.page_title): r for r in daily_article_views(pageviews).collect()}

    assert (daily["en", "A"].desktop_views, daily["en", "A"].mobile_views, daily["en", "A"].views) == (
        17,
        5,
        22,
    )
    assert ("en", "Main_Page") not in daily

    top = {
        (r.language, r.rank): r.page_title
        for r in top_articles(daily_article_views(pageviews), n=2).collect()
    }
    assert top == {("en", 1): "A", ("en", 2): "B", ("de", 1): "X"}


def test_trending_uses_zero_filled_seven_day_baseline(spark):
    d0 = date(2025, 10, 1)
    day = [d0 + timedelta(days=i) for i in range(9)]
    rows = [(d, "en", "wikipedia", "Steady", "Steady", 2000) for d in day]
    rows += [
        (day[1], "en", "wikipedia", "Spike", "Spike", 3500),
        (day[8], "en", "wikipedia", "Spike", "Spike", 7000),  # baseline 3500/7 = 500 -> 14x
        (day[8], "en", "wikipedia", "Newcomer", "Newcomer", 5000),  # no history -> new page
        (day[8], "en", "wikipedia", "Small", "Small", 500),  # spike, but below min_views
    ]
    trending = {
        r.page_title: r for r in trending_articles(spark.createDataFrame(rows, DAILY_SCHEMA)).collect()
    }

    assert set(trending) == {"Spike", "Newcomer"}
    assert (trending["Spike"].baseline_views, trending["Spike"].spike_ratio) == (500.0, 14.0)
    assert trending["Spike"].days_of_history == 7
    assert trending["Newcomer"].spike_ratio is None


def test_first_loaded_day_is_never_trending(spark):
    rows = [(date(2025, 10, 1), "en", "wikipedia", "Anything", "Anything", 50000)]

    assert trending_articles(spark.createDataFrame(rows, DAILY_SCHEMA)).count() == 0


def test_hourly_traffic_mobile_share_and_wiki_names(spark):
    pageviews = clean_pageviews(bronze(spark, "en A 30 0", "en.m A 70 0", "en.m B 100 0", "de X 5 0"))
    wikis = spark.createDataFrame(
        [("en", "English", "wikipedia", "https://en.wikipedia.org", "enwiki")],
        "language_code string, language_name string, project string, site_url string, dbname string",
    )
    traffic = {r.language: r for r in hourly_traffic(pageviews, wikis).collect()}

    en = traffic["en"]
    assert (en.views, en.mobile_views, en.mobile_share, en.approx_distinct_pages) == (200, 170, 0.85, 2)
    assert (en.language_name, en.site_url) == ("English", "https://en.wikipedia.org")
    assert traffic["de"].language_name is None  # unknown wikis are kept, not dropped


def test_quality_summary_counts_rule_failures(spark):
    df = clean_pageviews(bronze(spark, "en A 1 0", "en B 2 0", "en C 0 0", "en - 5 0"))
    (day,) = quality_summary(*split(df)).collect()

    assert day.view_date == date(2025, 10, 1)
    assert (day.passed, day.quarantined) == (2, 2)
    assert day.failed_positive_views == 1
    assert day.failed_has_title == 1
    assert day.failed_well_formed == 0
    assert day.quarantine_pct == 50.0
