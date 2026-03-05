# Structured AI Lakehouse Integrations

Production-ready scripts and examples for exporting data from Structured AI's Lakehouse Direct Access to external storage systems.

> **Part of [Structured AI Examples](https://github.com/Structured-AI/structured-ai-examples)** - Visit the main repository for other integration examples.

## Overview

This repository provides Python scripts and implementation guides for exporting data from Structured AI's Lakehouse to various cloud storage providers. All examples use Direct Access for secure, efficient data transfer.

## Quick Start

```bash
# Clone this repository
git clone https://github.com/Structured-AI/structured-ai-lakehouse-integrations.git
cd structured-ai-lakehouse-integrations

# Install dependencies for your integration (each integration has its own requirements)
pip install -r integrations/delta-sharing-to-aws-s3/requirements.txt

# Configure your Delta Sharing profile
cp config.example.json config.share
# Edit config.share with your endpoint and bearer token

# Run the export script (see integrations/delta-sharing-to-aws-s3/README.md for full options)
python integrations/delta-sharing-to-aws-s3/export_to_s3.py \
    --profile config.share \
    --s3-bucket your-bucket \
    --s3-prefix exports \
    --aws-region us-east-1
```

## Supported Integrations

- **AWS S3** — Export data to Amazon S3 buckets via Delta Sharing ([Setup Guide](integrations/delta-sharing-to-aws-s3/README.md))
- **Azure Blob Storage** — Push data to Azure Storage (coming soon)
- **Google Cloud Storage** — Sync to GCS buckets (coming soon)

## Documentation

- [AWS S3 Export Guide](integrations/delta-sharing-to-aws-s3/README.md) — Step-by-step instructions for exporting via Delta Sharing to S3

## Requirements

- Python 3.8+
- Structured AI Lakehouse Direct Access credentials
- Cloud storage provider credentials (AWS, Azure, or GCP)

Each integration has its own `requirements.txt` with the dependencies it needs. Install only what you use.

## Support

For questions or issues:
- Check the [AWS S3 Export Guide](integrations/delta-sharing-to-aws-s3/README.md#troubleshooting) troubleshooting section
- Contact Structured AI support: support@structured.ai
- Visit [Structured AI Documentation](https://docs.structured.ai)

## License

MIT License - See [LICENSE](LICENSE) for details.
