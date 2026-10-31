"""Why is it trending? A daily digest of English Wikipedia's biggest spikes.

Reads gold_trending_stories (written by the build_digest job task) through a SQL warehouse, as the
app's service principal.
"""

import os
import urllib.parse

import pandas as pd
import streamlit as st
from databricks import sql
from databricks.sdk.core import Config

TABLE = os.environ["STORIES_TABLE"]
WAREHOUSE_ID = os.environ["DATABRICKS_WAREHOUSE_ID"]
cfg = Config()


@st.cache_resource
def connection():
    return sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{WAREHOUSE_ID}",
        credentials_provider=lambda: cfg.authenticate,
    )


@st.cache_data(ttl=600)
def query(statement: str, **params) -> pd.DataFrame:
    with connection().cursor() as cursor:
        cursor.execute(statement, params)
        return cursor.fetchall_arrow().to_pandas()


def compact(n: float) -> str:
    return f"{n / 1e6:.1f}M" if n >= 1e6 else f"{n / 1e3:.0f}k" if n >= 1e3 else f"{n:.0f}"


def article_line(a: dict) -> str:
    url = "https://en.wikipedia.org/wiki/" + urllib.parse.quote(a["page_title"], safe="")
    title = a["article"].replace("[", r"\[").replace("]", r"\]")
    jump = f"{a['spike_ratio']:g}× usual" if a["spike_ratio"] else "new"
    return f"- [{title}]({url}) · {compact(a['views'])} views · {jump}"


st.set_page_config(page_title="Why is it trending?", page_icon="📈")
st.title("Why is it trending?")
st.caption(
    "English Wikipedia articles that got at least 5× their usual daily views, grouped into stories. "
    "An LLM writes each explanation from Wikipedia's own Current events page and article summaries."
)

try:
    days = query("SELECT DISTINCT view_date FROM IDENTIFIER(:table) ORDER BY view_date DESC", table=TABLE)
    dates = days["view_date"]
except Exception as err:
    # The job creates the table on its first run, which can be after the app's first deploy.
    if "TABLE_OR_VIEW_NOT_FOUND" not in str(err):
        raise
    dates = pd.Series(dtype=object)
if dates.empty:
    st.info("No digest yet. It's written daily after the pipeline loads a new day.")
    st.stop()

day = st.selectbox("Day (UTC)", list(dates), format_func=lambda d: f"{d:%A, %B} {d.day}, {d.year}")
rows = query(
    "SELECT * FROM IDENTIFIER(:table) WHERE view_date = :day ORDER BY story_rank", table=TABLE, day=day
)

for story in rows[rows["kind"] == "story"].itertuples():
    with st.container(border=True):
        st.subheader(story.headline)
        st.caption(
            f"{story.category.replace('_', ' ')} · {story.confidence or 'unrated'} confidence · "
            f"{compact(story.total_views)} views"
        )
        st.write(story.summary)
        st.markdown("\n".join(article_line(a) for a in story.articles))

leftovers = {
    "unexplained": "Also trending, no clear reason in the news",
    "likely_automated": "Set aside as likely automated traffic (almost all desktop, or one hour)",
}
for kind, label in leftovers.items():
    for group in rows[rows["kind"] == kind].itertuples():
        with st.expander(f"{label} ({len(group.articles)})"):
            st.markdown("\n".join(article_line(a) for a in group.articles))

generated = rows["generated_at"].max()
model = rows["model"].dropna()
st.caption(
    f"Generated {generated:%Y-%m-%d %H:%M} UTC" + (f" by {model.iloc[0]}" if not model.empty else "") + ". "
    "Data: Wikimedia pageview dumps, CC0."
)
