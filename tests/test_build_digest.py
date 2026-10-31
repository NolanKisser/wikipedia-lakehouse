from datetime import UTC, date, datetime

import pytest

from build_digest import (
    build_messages,
    clean_wikitext,
    current_events_title,
    is_likely_automated,
    linked_articles,
    parse_stories,
    story_rows,
    trending_candidates,
)

DAY = date(2026, 10, 29)
TRENDING_SCHEMA = (
    "view_date date, language string, project string, page_title string, article string, "
    "views bigint, baseline_views double, spike_ratio double"
)
PAGEVIEWS_SCHEMA = (
    "view_hour timestamp, view_date date, language string, project string, access string, "
    "page_title string, is_content boolean, views bigint"
)


def candidate(article, views=10_000, **extra):
    return {"page_title": article.replace(" ", "_"), "article": article, "views": views, **extra}


def test_candidates_are_top_english_articles_with_peak_hour_and_desktop_share(spark):
    trending = spark.createDataFrame(
        [
            (DAY, "en", "wikipedia", "Cohere", "Cohere", 9000, 500.0, 18.0),
            (DAY, "en", "wikipedia", "Instacart", "Instacart", 4000, 400.0, 10.0),
            (DAY, "en", "wikipedia", "Quiet", "Quiet", 1000, 100.0, 10.0),  # cut by the limit
            (DAY, "de", "wikipedia", "Cohere", "Cohere", 99999, 1.0, 99.0),  # other wiki
            (date(2026, 10, 28), "en", "wikipedia", "Old", "Old", 99999, 1.0, 99.0),  # other day
        ],
        TRENDING_SCHEMA,
    )
    hour = lambda h: datetime(2026, 10, 29, h, tzinfo=UTC)  # noqa: E731
    pageviews = spark.createDataFrame(
        [
            (hour(14), DAY, "en", "wikipedia", "mobile", "Cohere", True, 5000),
            (hour(14), DAY, "en", "wikipedia", "desktop", "Cohere", True, 1000),
            (hour(9), DAY, "en", "wikipedia", "desktop", "Cohere", True, 3000),
            (hour(9), DAY, "de", "wikipedia", "desktop", "Cohere", True, 99999),
        ],
        PAGEVIEWS_SCHEMA,
    )

    rows = trending_candidates(trending, pageviews, DAY, limit=2).collect()

    assert [r.page_title for r in rows] == ["Cohere", "Instacart"]
    cohere, instacart = rows
    assert (cohere.peak_hour, cohere.desktop_share) == (14, 4000 / 9000)
    assert (instacart.peak_hour, instacart.desktop_share) == (None, None)  # no hourly rows: left join


@pytest.mark.parametrize(
    "desktop_share, expected",
    # Real Oct 29 2026 shares: Digital signage (bot), Patrick Stewart, no hourly rows.
    [(1.0, True), (0.95, True), (0.23, False), (None, False)],
)
def test_likely_automated(desktop_share, expected):
    assert is_likely_automated(candidate("X", desktop_share=desktop_share)) is expected


def test_clean_wikitext_keeps_text_and_link_targets():
    raw = (
        "{{Current events|year=2026}}\n<!-- hidden -->\n'''Business and economy'''\n"
        "*[[Christine Lagarde]], the [[president of the European Central Bank|ECB president]], "
        "resigns.<ref>x</ref> [https://www.reuters.com/x (Reuters)]\n\n"
        "**[[Cohere|cohere]] raises money."
    )

    assert clean_wikitext(raw) == (
        "Business and economy\n"
        "*Christine Lagarde, the ECB president (president of the European Central Bank), resigns.\n"
        "**cohere raises money."
    )


def test_current_events_title_has_no_zero_padding():
    assert current_events_title(date(2026, 3, 5)) == "Portal:Current_events/2026_March_5"


def test_messages_include_context_and_every_candidate():
    candidates = [
        candidate(
            "Cohere", 9000, spike_ratio=18.0, peak_hour=14, description="AI company", extract="Cohere is"
        ),
        candidate("Brand New", 2000, spike_ratio=None, linked=["Cohere", "Toronto"]),
    ]
    messages = build_messages(DAY, {date(2026, 10, 28): "Lagarde resigns", DAY: None}, candidates)

    system, user = messages
    assert system["role"] == "system" and "JSON only" in system["content"]
    assert user["content"].index("2026-10-28") < user["content"].index("(not available)")
    assert "- Cohere: 9,000 views, 18.0x usual, peak 14:00 UTC" in user["content"]
    assert "Description: AI company" in user["content"]
    assert (
        "- Brand New: 2,000 views, new (no views before)\n  Linked with: Cohere, Toronto" in user["content"]
    )


def test_linked_articles_are_symmetric_and_limited_to_candidates():
    links = [
        ("Wendy Neuss", "Patrick Stewart"),  # only one direction exists on Wikipedia
        ("Patrick Stewart", "Jean-Luc Picard"),
        ("Patrick Stewart", "Yorkshire"),  # not trending
        ("Jean-Luc Picard", "Jean-Luc_Picard"),  # self link after normalising
    ]
    articles = ["Patrick Stewart", "Wendy Neuss", "Jean-Luc Picard", "Halloween"]

    assert linked_articles(links, articles) == {
        "Patrick Stewart": ["Jean-Luc Picard", "Wendy Neuss"],
        "Wendy Neuss": ["Patrick Stewart"],
        "Jean-Luc Picard": ["Patrick Stewart"],
    }


def test_parse_stories_validates_titles_and_fields():
    candidates = [candidate(t) for t in ["Cohere", "Instacart", "DoorDash", "Toronto", "Halloween"]]
    reply = """```json
    {"articles": [
      {"title": "doordash", "story": 1},
      {"title": "Instacart", "story": "1"},
      {"title": "Instacart", "story": 2},
      {"title": "Not In List", "story": 1},
      {"title": "Cohere", "story": 2},
      {"title": "Toronto", "story": 3},
      {"title": "Halloween", "story": null}
    ],
    "stories": [
      {"id": 1, "headline": "DoorDash to buy Instacart", "summary": "A $17B deal.", "category": "news",
       "confidence": "high"},
      {"id": 2, "headline": "Canada invests in Cohere", "summary": "C$1.6B stake.", "category": "finance",
       "confidence": "low"},
      {"id": 3, "headline": "", "summary": "No headline, so Toronto stays unassigned."},
      {"id": 4, "headline": "No articles", "summary": "Dropped."}
    ]}
    ```"""

    stories = parse_stories(reply, candidates)

    assert [s["headline"] for s in stories] == ["DoorDash to buy Instacart", "Canada invests in Cohere"]
    assert [a["article"] for a in stories[0]["articles"]] == ["DoorDash", "Instacart"]  # ids as int or str
    assert [a["article"] for a in stories[1]["articles"]] == ["Cohere"]  # repeated Instacart ignored
    assert (stories[1]["category"], stories[1]["confidence"]) == ("other", None)


def test_parse_stories_rejects_replies_without_json():
    with pytest.raises(ValueError, match="No JSON"):
        parse_stories("Sorry, I can't help with that.", [])


def test_story_rows_rank_stories_by_views_then_leftovers():
    small = {
        "headline": "Small",
        "summary": "s",
        "category": "news",
        "confidence": "high",
        "articles": [candidate("A", 2000)],
    }
    big = {
        "headline": "Big",
        "summary": "b",
        "category": "sports",
        "confidence": "medium",
        "articles": [candidate("B", 3000), candidate("C", 4000, spike_ratio=6.0)],
    }
    now = datetime(2026, 10, 30, 6, tzinfo=UTC)

    rows = story_rows(DAY, [small, big], [candidate("D", 9000)], [], "m", now)

    assert [(r["story_rank"], r["kind"], r["headline"], r["total_views"]) for r in rows] == [
        (1, "story", "Big", 7000),
        (2, "story", "Small", 2000),
        (3, "unexplained", None, 9000),
    ]
    assert rows[0]["articles"][1] == {
        "page_title": "C",
        "article": "C",
        "views": 4000,
        "baseline_views": None,
        "spike_ratio": 6.0,
        "peak_hour": None,
        "desktop_share": None,
    }
    assert rows[0]["model"] == "m" and rows[2]["model"] is None
