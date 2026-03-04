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

# Install dependencies
pip install -r requirements.txt

# Configure your credentials
cp config.example.json config.json
# Edit config.json with your credentials

# Run the export script
python export_to_s3.py
```

## Supported Integrations

- **AWS S3** - Export data to Amazon S3 buckets
- **Azure Blob Storage** - Push data to Azure Storage (coming soon)
- **Google Cloud Storage** - Sync to GCS buckets (coming soon)

## Documentation

Detailed setup guides and API documentation are available in the `/docs` folder:
- [Getting Started Guide](docs/getting-started.md)
- [AWS S3 Setup](docs/aws-s3-setup.md)
- [Authentication Guide](docs/authentication.md)
- [Troubleshooting](docs/troubleshooting.md)

## Requirements

- Python 3.8+
- Structured AI Lakehouse Direct Access credentials
- Cloud storage provider credentials (AWS, Azure, or GCP)

## Support

For questions or issues:
- Check the [Troubleshooting Guide](docs/troubleshooting.md)
- Contact Structured AI support: support@structured.ai
- Visit [Structured AI Documentation](https://docs.structured.ai)

## License

MIT License - See [LICENSE](LICENSE) for details.
