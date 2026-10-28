"""Create (or refresh) a Lakehouse Monitoring time-series profile on a table.

Runs after the pipeline so the table exists on the very first deploy. Later runs just
trigger a refresh, which recomputes the profile and drift metrics tables.
"""

import argparse

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.catalog import MonitorTimeSeries


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--table", required=True, help="catalog.schema.table to monitor")
    parser.add_argument("--timestamp-col", required=True, help="Timestamp column for the time series")
    parser.add_argument("--granularity", default="1 day", help='e.g. "1 hour", "1 day"')
    parser.add_argument(
        "--slice", action="append", default=[], dest="slices", help="Column to slice by; repeatable"
    )
    parser.add_argument("--output-schema", required=True, help="catalog.schema for metric tables")
    parser.add_argument("--assets-dir", required=True, help="Workspace folder for the monitor dashboard")
    args = parser.parse_args()

    assets_dir = args.assets_dir
    if not assets_dir.startswith("/Workspace"):
        assets_dir = "/Workspace" + assets_dir

    w = WorkspaceClient()
    try:
        w.quality_monitors.get(table_name=args.table)
    except NotFound:
        w.quality_monitors.create(
            table_name=args.table,
            assets_dir=assets_dir,
            output_schema_name=args.output_schema,
            time_series=MonitorTimeSeries(timestamp_col=args.timestamp_col, granularities=[args.granularity]),
            slicing_exprs=args.slices or None,
        )
        print(f"Created monitor on {args.table}; the first refresh starts automatically")
        return

    refresh = w.quality_monitors.run_refresh(table_name=args.table)
    print(f"Started refresh {refresh.refresh_id} on {args.table}")


if __name__ == "__main__":
    main()
