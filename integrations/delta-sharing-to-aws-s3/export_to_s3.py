"""
export_to_s3.py
---------------
Export tables from Structured AI Lakehouse via Delta Sharing to AWS S3.

Connects to a Delta Sharing endpoint using a .share profile file, discovers
tables in the specified share and schema, loads each into a pandas DataFrame,
and uploads to S3 as Parquet with date partitioning for incremental loads.

S3 path layout:
  s3://<bucket>/<base-prefix>/<schema>/<table>/snapshot_date=YYYY-MM-DD/data.parquet

Usage:
  python export_to_s3.py \\
      --profile        /path/to/your.share \\
      --s3-bucket      my-bucket \\
      --s3-prefix      exports \\
      --aws-region     us-east-1 \\
      [--share         <share_name>] \\
      [--schema        <schema_name>] \\
      [--table         <table_name>] \\
      [--limit         <row_limit>] \\
      [--fetch-only]

Profile file (.share) format:
  {
    "shareCredentialsVersion": 1,
    "endpoint": "https://<delta-sharing-endpoint>/delta-sharing/",
    "bearerToken": "<your-bearer-token>",
    "expirationTime": "2027-01-01T00:00:00.000Z"
  }
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import date
from pathlib import Path

# ---------------------------------------------------------------------------
# Dependency guard
# ---------------------------------------------------------------------------

try:
    import delta_sharing
    import pandas as pd
except ImportError as exc:
    print(
        f"[ERROR] Missing dependency: {exc}\n"
        "Install with:  pip install -r integrations/delta-sharing-to-aws-s3/requirements.txt",
        file=sys.stderr,
    )
    sys.exit(1)


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

def _setup_logging() -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export Delta Sharing tables to S3 as Parquet.",
        formatter_class=argparse.RawTextHelpFormatter,
    )

    ds = parser.add_argument_group("Delta Sharing")
    ds.add_argument("--profile", "-p", required=True,
                    help="Path to the .share credential file.")
    ds.add_argument("--share", "-s", default="structured_tenant_share",
                    help="Share name (default: structured_tenant_share).")
    ds.add_argument("--schema", default="mart_v1",
                    help="Schema inside the share (default: mart_v1).")
    ds.add_argument("--table", "-t", default="",
                    help="Export only this table; omit to export all tables in the schema.")
    ds.add_argument("--limit", "-l", type=int, default=0,
                    help="Row limit per table (0 = no limit).")

    s3 = parser.add_argument_group("S3")
    s3.add_argument("--s3-bucket", default="",
                    help="S3 bucket name.")
    s3.add_argument("--s3-prefix", default="",
                    help="Base prefix inside the bucket.")
    s3.add_argument("--aws-region", default="",
                    help="AWS region (e.g. us-east-1). Falls back to AWS_DEFAULT_REGION.")
    s3.add_argument("--aws-access-key", default="",
                    help="AWS access key ID. Falls back to AWS_ACCESS_KEY_ID env var.")
    s3.add_argument("--aws-secret-key", default="",
                    help="AWS secret access key. Falls back to AWS_SECRET_ACCESS_KEY env var.")
    s3.add_argument("--snapshot-date", default="",
                    help="Override partition date (YYYY-MM-DD). Defaults to today.")
    s3.add_argument("--fetch-only", action="store_true",
                    help="Fetch tables and print row counts without uploading to S3.")

    return parser.parse_args()


# ---------------------------------------------------------------------------
# Delta Sharing helpers
# ---------------------------------------------------------------------------

def _resolve_profile(profile_path: str) -> str:
    path = Path(profile_path).expanduser().resolve()
    if not path.exists():
        raise FileNotFoundError(f"Profile file not found: {path}")
    return str(path)


def _list_tables(client: delta_sharing.SharingClient, share: str, schema: str) -> list[str]:
    """Return table names for the given share + schema."""
    try:
        all_tables = client.list_all_tables()
    except Exception as exc:
        raise RuntimeError(f"Failed to list tables: {exc}") from exc

    matched = [t.name for t in all_tables if t.share == share and t.schema == schema]

    if not matched:
        available = ", ".join(f"{t.share}/{t.schema}" for t in all_tables)
        logging.warning(
            "No tables found in share='%s' schema='%s'. Available: %s",
            share, schema, available,
        )
    return matched


def fetch_table(profile_path: str, share: str, schema: str, table: str, limit: int) -> pd.DataFrame:
    """
    Load a single Delta Sharing table as a pandas DataFrame.

    The core data fetch uses delta_sharing.load_as_pandas() with a table URL
    in the format: <profile_path>#<share>.<schema>.<table>
    """
    # ── DATA QUERY ──────────────────────────────────────────────────────────
    # This is the core query executed against the Delta Sharing endpoint.
    # table_url format: <profile_path>#<share>.<schema>.<table>
    table_url = f"{profile_path}#{share}.{schema}.{table}"
    if limit > 0:
        df = delta_sharing.load_as_pandas(table_url, limit=limit)
    else:
        df = delta_sharing.load_as_pandas(table_url)
    # ────────────────────────────────────────────────────────────────────────
    return df


def fetch_all_tables(
    profile_path: str,
    share: str,
    schema: str,
    table_names: list[str],
    limit: int,
    log: logging.Logger,
) -> dict[str, pd.DataFrame]:
    """Fetch all tables; returns {table_name: DataFrame}. Failed tables are skipped."""
    dataframes: dict[str, pd.DataFrame] = {}

    for table in table_names:
        log.info("[%s] Fetching…", table)
        t0 = time.perf_counter()
        try:
            df = fetch_table(profile_path, share, schema, table, limit)
            elapsed = time.perf_counter() - t0
            log.info("[%s] Fetched %s rows × %s cols [%.1fs]", table, f"{len(df):,}", len(df.columns), elapsed)
            dataframes[table] = df
        except Exception as exc:
            elapsed = time.perf_counter() - t0
            log.error("[%s] FAILED after %.1fs — %s", table, elapsed, exc)

    return dataframes


# ---------------------------------------------------------------------------
# S3 helpers
# ---------------------------------------------------------------------------

def _build_s3_storage_options(access_key: str, secret_key: str, region: str) -> dict:
    """Build s3fs storage_options dict from explicit creds or env vars."""
    opts: dict = {}

    key = access_key or os.environ.get("AWS_ACCESS_KEY_ID", "")
    secret = secret_key or os.environ.get("AWS_SECRET_ACCESS_KEY", "")
    reg = region or os.environ.get("AWS_DEFAULT_REGION", "")

    if key:
        opts["key"] = key
    if secret:
        opts["secret"] = secret
    if reg:
        opts["client_kwargs"] = {"region_name": reg}

    return opts


def _s3_partition_key(base_prefix: str, schema: str, table: str, snapshot_date: str) -> str:
    """Build the S3 object key for the Parquet file."""
    parts = [p for p in [base_prefix, schema, table] if p]
    return "/".join(parts) + f"/snapshot_date={snapshot_date}/data.parquet"


def upload_to_s3(
    df: pd.DataFrame,
    table: str,
    schema: str,
    bucket: str,
    base_prefix: str,
    snapshot_date: str,
    storage_options: dict,
) -> str:
    """
    Write a DataFrame to S3 as Parquet (snappy compressed).
    Returns the full s3:// URI written.
    Overwrites any existing file for the same snapshot_date partition (idempotent reruns).
    """
    try:
        import s3fs  # noqa: F401 — validates install; pandas uses it via storage_options
    except ImportError:
        raise ImportError("s3fs not installed. Run: pip install s3fs") from None

    key = _s3_partition_key(base_prefix, schema, table, snapshot_date)
    s3_uri = f"s3://{bucket}/{key}"

    df.to_parquet(
        s3_uri,
        engine="pyarrow",
        index=False,
        compression="snappy",
        storage_options=storage_options,
    )
    return s3_uri


def upload_all_to_s3(
    dataframes: dict[str, pd.DataFrame],
    schema: str,
    bucket: str,
    base_prefix: str,
    snapshot_date: str,
    storage_options: dict,
    log: logging.Logger,
) -> dict[str, str]:
    """Upload all fetched DataFrames to S3. Returns {table_name: s3_uri}."""
    uploaded: dict[str, str] = {}

    for table, df in dataframes.items():
        log.info("[%s] Uploading to S3…", table)
        t0 = time.perf_counter()
        try:
            uri = upload_to_s3(df, table, schema, bucket, base_prefix, snapshot_date, storage_options)
            elapsed = time.perf_counter() - t0
            log.info("[%s] Uploaded %s rows → %s [%.1fs]", table, f"{len(df):,}", uri, elapsed)
            uploaded[table] = uri
        except Exception as exc:
            elapsed = time.perf_counter() - t0
            log.error("[%s] Upload FAILED after %.1fs — %s", table, elapsed, exc)

    return uploaded


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    log = _setup_logging()
    args = _parse_args()

    try:
        profile_path = _resolve_profile(args.profile)
    except FileNotFoundError as e:
        log.error("%s", e)
        sys.exit(1)

    snapshot_date = args.snapshot_date or date.today().isoformat()
    fetch_only = args.fetch_only or not args.s3_bucket

    log.info("=" * 65)
    log.info("Delta Sharing → S3 Export")
    log.info("  Profile       : %s", profile_path)
    log.info("  Share         : %s", args.share)
    log.info("  Schema        : %s", args.schema)
    log.info("  Row limit     : %s", args.limit if args.limit > 0 else "none")
    log.info("  Snapshot date : %s", snapshot_date)
    if not fetch_only:
        log.info(
            "  S3 target     : s3://%s/%s/%s/<table>/snapshot_date=%s/data.parquet",
            args.s3_bucket, args.s3_prefix, args.schema, snapshot_date,
        )
    else:
        log.info("  S3 upload     : skipped (--fetch-only or no --s3-bucket provided)")
    log.info("=" * 65)

    client = delta_sharing.SharingClient(profile_path)

    if args.table:
        tables = [args.table]
        log.info("Targeting single table: %s", args.table)
    else:
        try:
            tables = _list_tables(client, args.share, args.schema)
        except RuntimeError as e:
            log.error("%s", e)
            sys.exit(1)
        log.info("Discovered %s table(s): %s", len(tables), tables)

    if not tables:
        log.info("No tables to fetch. Exiting.")
        sys.exit(0)

    dataframes = fetch_all_tables(
        profile_path, args.share, args.schema, tables, args.limit, log,
    )

    fetch_ok = len(dataframes)
    fetch_failed = len(tables) - fetch_ok

    uploaded: dict[str, str] = {}
    if not fetch_only and dataframes:
        storage_options = _build_s3_storage_options(
            args.aws_access_key, args.aws_secret_key, args.aws_region,
        )
        log.info("Uploads are idempotent: existing files for snapshot_date=%s will be overwritten.", snapshot_date)
        uploaded = upload_all_to_s3(
            dataframes, args.schema, args.s3_bucket, args.s3_prefix,
            snapshot_date, storage_options, log,
        )

    log.info("=" * 65)
    log.info("Fetch  : %s succeeded, %s failed", fetch_ok, fetch_failed)
    if not fetch_only:
        log.info("Upload : %s succeeded, %s failed", len(uploaded), fetch_ok - len(uploaded))
    if uploaded:
        log.info("Files written:")
        for table, uri in uploaded.items():
            rows = len(dataframes[table])
            log.info("  %-40s  %8s rows  →  %s", table, f"{rows:,}", uri)
    log.info("=" * 65)

    if fetch_failed or (not fetch_only and len(uploaded) < fetch_ok):
        sys.exit(1)


if __name__ == "__main__":
    main()
