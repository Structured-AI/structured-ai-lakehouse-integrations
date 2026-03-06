# Delta Sharing → AWS S3 Export

Export tables from Structured AI Lakehouse via Delta Sharing to Amazon S3 as Parquet files.

## Overview

This script connects to a Delta Sharing endpoint, discovers tables in the specified share and schema, loads each table into memory, and uploads to S3 with date partitioning for incremental loads. Output files use Snappy compression and follow a consistent path layout for easy integration with downstream tools.

## Prerequisites

- Python 3.8+
- Delta Sharing profile file (`.share`) with valid credentials
- AWS credentials with write access to the target S3 bucket

## Obtain Your Delta Sharing Profile

Your `.share` profile file contains the endpoint and bearer token needed to access the Delta Sharing API. Obtain this file from your Structured AI administrator or through the platform's data access settings.

Save the file as `config.share` (or any name) and keep it secure. Do not commit it to version control.

**Profile file format:**

```json
{
  "shareCredentialsVersion": 1,
  "endpoint": "https://<delta-sharing-endpoint>/delta-sharing/",
  "bearerToken": "<your-bearer-token>",
  "expirationTime": "2027-01-01T00:00:00.000Z"
}
```

## AWS Credentials Setup

AWS credentials are resolved in this order:

1. **CLI arguments** — `--aws-access-key` and `--aws-secret-key`
2. **Environment variables** — `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`
3. **IAM role / credentials file** — boto3 default chain: `~/.aws/credentials`, instance role, etc.

**Example using environment variables:**

```bash
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
export AWS_DEFAULT_REGION="us-east-1"
```

## Install Dependencies

This integration has its own requirements. From the repository root:

```bash
pip install -r integrations/delta-sharing-to-aws-s3/requirements.txt
```

Or from the integration directory:

```bash
cd integrations/delta-sharing-to-aws-s3
pip install -r requirements.txt
```

## Data Query Reference

The script fetches data using the Delta Sharing Python client. The core query is:

```python
delta_sharing.load_as_pandas(table_url, limit=limit)
```

**Table URL format:**

```
<profile_path>#<share>.<schema>.<table>
```

Example: `/path/to/config.share#structured_tenant_share.mart_v1.user_profile_snapshot`

- `profile_path` — full path to your `.share` file
- `share` — share name (e.g. `structured_tenant_share`)
- `schema` — schema name (e.g. `mart_v1`)
- `table` — table name (e.g. `user_profile_snapshot`)

`limit` is optional. When `limit=0` (default), the full table is loaded. When `limit > 0`, only the first N rows are returned.

## Running the Script

### Export all tables in a schema to S3

```bash
python integrations/delta-sharing-to-aws-s3/export_to_s3.py \
    --profile /path/to/config.share \
    --s3-bucket my-bucket \
    --s3-prefix exports \
    --aws-region us-east-1
```

### Export a single table

```bash
python integrations/delta-sharing-to-aws-s3/export_to_s3.py \
    --profile /path/to/config.share \
    --s3-bucket my-bucket \
    --s3-prefix exports \
    --aws-region us-east-1 \
    --table user_profile_snapshot
```

### Fetch only (no S3 upload)

Use for validation or testing. Prints row counts without uploading.

```bash
python integrations/delta-sharing-to-aws-s3/export_to_s3.py \
    --profile /path/to/config.share \
    --fetch-only
```

### Custom date partition

Override the snapshot date for the output path:

```bash
python integrations/delta-sharing-to-aws-s3/export_to_s3.py \
    --profile /path/to/config.share \
    --s3-bucket my-bucket \
    --s3-prefix exports \
    --aws-region us-east-1 \
    --snapshot-date 2025-01-15
```

### Custom share and schema

```bash
python integrations/delta-sharing-to-aws-s3/export_to_s3.py \
    --profile /path/to/config.share \
    --s3-bucket my-bucket \
    --s3-prefix exports \
    --aws-region us-east-1 \
    --share my_share \
    --schema my_schema
```

### Limit rows per table (for testing)

```bash
python integrations/delta-sharing-to-aws-s3/export_to_s3.py \
    --profile /path/to/config.share \
    --s3-bucket my-bucket \
    --s3-prefix exports \
    --aws-region us-east-1 \
    --limit 1000
```

## Output Layout

Files are written to:

```
s3://<bucket>/<prefix>/<schema>/<table>/snapshot_date=<YYYY-MM-DD>/data.parquet
```

**Example:**

```
s3://my-bucket/exports/mart_v1/user_profile_snapshot/snapshot_date=2025-03-05/data.parquet
```

- **Format:** Parquet (Snappy compression)
- **Idempotent:** Re-running with the same `snapshot_date` overwrites the existing file

## Command-Line Reference

| Argument | Required | Default | Description |
|----------|----------|---------|-------------|
| `--profile`, `-p` | Yes | — | Path to `.share` credential file |
| `--share`, `-s` | No | `structured_tenant_share` | Share name |
| `--schema` | No | `mart_v1` | Schema name |
| `--table`, `-t` | No | (all) | Fetch only this table |
| `--limit`, `-l` | No | 0 | Row limit per table (0 = no limit) |
| `--s3-bucket` | No* | — | S3 bucket name |
| `--s3-prefix` | No | — | Base prefix inside bucket |
| `--aws-region` | No | env | AWS region |
| `--aws-access-key` | No | env | AWS access key ID |
| `--aws-secret-key` | No | env | AWS secret access key |
| `--snapshot-date` | No | today | Partition date (YYYY-MM-DD) |
| `--fetch-only` | No | false | Skip S3 upload |

\* Required when not using `--fetch-only`

## Troubleshooting

| Issue | Solution |
|-------|----------|
| `Profile file not found` | Ensure `--profile` path is correct and the file exists |
| `Failed to list tables` | Check profile credentials and endpoint URL; verify token expiry |
| `s3fs not installed` | Run `pip install s3fs` |
| S3 upload fails | Verify AWS credentials and bucket permissions; check region |
| Token expired | Obtain a new `.share` profile from your administrator |

For additional support, contact Structured AI support: support@structured.ai
