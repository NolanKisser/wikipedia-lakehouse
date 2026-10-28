-- Share the gold layer with someone outside your workspace via Delta Sharing.
-- Run in a SQL editor as a user with CREATE SHARE / CREATE RECIPIENT on the metastore.
-- Replace workspace.wikipedia with your catalog.schema (dev deploys use dev_<you>_wikipedia).

CREATE SHARE IF NOT EXISTS wikipedia_gold
  COMMENT 'Daily top and trending Wikipedia articles';

-- Materialized views are shareable on current Unity Catalog; if your workspace rejects them,
-- share silver_pageviews instead.
ALTER SHARE wikipedia_gold ADD MATERIALIZED VIEW workspace.wikipedia.gold_daily_top_articles;
ALTER SHARE wikipedia_gold ADD MATERIALIZED VIEW workspace.wikipedia.gold_trending_articles;

-- Open sharing: the recipient gets an activation link and can read the data from pandas,
-- Spark, Power BI, etc. using the open-source delta-sharing client, no Databricks required.
CREATE RECIPIENT IF NOT EXISTS wiki_partner
  COMMENT 'External consumer of the gold Wikipedia tables';

GRANT SELECT ON SHARE wikipedia_gold TO RECIPIENT wiki_partner;

-- Copy activation_link from this output and send it to the recipient.
DESCRIBE RECIPIENT wiki_partner;
