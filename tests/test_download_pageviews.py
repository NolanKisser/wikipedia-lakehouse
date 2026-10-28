from datetime import UTC, date, datetime

from download_pageviews import expand_hours, flatten_sitematrix, hour_file_name, hour_url


def utc(*args):
    return datetime(*args, tzinfo=UTC)


def test_day_expands_to_24_hours():
    hours = expand_hours("2025-10-01")

    assert len(hours) == 24
    assert (hours[0], hours[-1]) == (utc(2025, 10, 1, 0), utc(2025, 10, 1, 23))


def test_day_range_crosses_year_boundary():
    hours = expand_hours("2024-12-31:2025-01-01")

    assert len(hours) == 48
    assert hours[24] == utc(2025, 1, 1, 0)


def test_single_hours_and_lists():
    assert expand_hours("2025-10-01T13, 2025-10-02T00,") == [utc(2025, 10, 1, 13), utc(2025, 10, 2, 0)]


def test_last_n_days_are_full_days_before_today():
    hours = expand_hours("last:2", today=date(2025, 3, 1))

    assert len(hours) == 48
    assert (hours[0], hours[-1]) == (utc(2025, 2, 27, 0), utc(2025, 2, 28, 23))


def test_hour_url_and_file_name():
    hour = utc(2025, 1, 5, 7)

    assert hour_file_name(hour) == "pageviews-20250105-070000.gz"
    assert hour_url(hour) == (
        "https://dumps.wikimedia.org/other/pageviews/2025/2025-01/pageviews-20250105-070000.gz"
    )


SITEMATRIX = {
    "sitematrix": {
        "count": 3,
        "0": {
            "code": "en",
            "name": "English",
            "localname": "English",
            "site": [
                {"url": "https://en.wikipedia.org", "dbname": "enwiki", "code": "wiki"},
                {"url": "https://en.wiktionary.org", "dbname": "enwiktionary", "code": "wiktionary"},
            ],
        },
        "1": {
            "code": "de",
            "name": "Deutsch",
            "localname": "German",
            "site": [{"url": "https://de.wikipedia.org", "dbname": "dewiki", "code": "wiki"}],
        },
        "specials": [
            {"url": "https://commons.wikimedia.org", "dbname": "commonswiki", "code": "commons"},
            {"url": "https://www.wikidata.org", "dbname": "wikidatawiki", "code": "wikidata"},
            {"url": "https://www.wikisource.org", "dbname": "sourceswiki", "code": "sources"},
        ],
    }
}


def test_flatten_sitematrix():
    assert flatten_sitematrix(SITEMATRIX) == [
        # Specials keyed like their pageview domain codes ("commons.m", "www.wd"); others skipped.
        {
            "language_code": "commons",
            "language_name": "Multilingual",
            "project": "wikimedia",
            "site_url": "https://commons.wikimedia.org",
            "dbname": "commonswiki",
        },
        {
            "language_code": "www",
            "language_name": "Multilingual",
            "project": "wikidata",
            "site_url": "https://www.wikidata.org",
            "dbname": "wikidatawiki",
        },
        {
            "language_code": "en",
            "language_name": "English",
            "project": "wikipedia",
            "site_url": "https://en.wikipedia.org",
            "dbname": "enwiki",
        },
        {
            "language_code": "en",
            "language_name": "English",
            "project": "wiktionary",
            "site_url": "https://en.wiktionary.org",
            "dbname": "enwiktionary",
        },
        {
            "language_code": "de",
            "language_name": "German",
            "project": "wikipedia",
            "site_url": "https://de.wikipedia.org",
            "dbname": "dewiki",
        },
    ]
