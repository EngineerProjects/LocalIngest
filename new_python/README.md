# Construction Data Pipeline

PySpark data pipeline for Construction insurance data (PTF_MVT, Capitaux, Emissions).

---

## Overview

Transforms raw CSV data through Bronze→Silver→Gold medallion architecture:

| Layer | Content |
|-------|---------|
| **Bronze** | Raw CSV files (monthly + reference) |
| **Silver** | Cleaned Parquet with schemas |
| **Gold** | Business-ready analytics |

---

## Quick Start

```bash
# Run all enabled components
python main.py --vision 202509

# Run specific component
python main.py --vision 202509 --component ptf_mvt
python main.py --vision 202509 --component capitaux
python main.py --vision 202509 --component emissions
```

---

## Output Datasets

| # | Pipeline | Dataset | Layer |
|---|----------|---------|-------|
| 1 | PTF_MVT | `mvt_ptf_{vision}` | Gold |
| 2 | PTF_MVT | `ird_risk_q45_{vision}` | Gold |
| 3 | PTF_MVT | `ird_risk_q46_{vision}` | Gold |
| 4 | Capitaux | `az_azec_capitaux_{vision}` | Gold |
| 5 | Emissions | `primes_emises_{vision}_pol_garp` | Gold |
| 6 | Emissions | `primes_emises_{vision}_pol` | Gold |

---

## Project Structure

```
new_python/
├── config/                     # Configuration files
│   ├── config.yml              # Paths, Spark, logging
│   ├── reading_config.json     # File patterns and schemas
│   ├── schemas.py              # PySpark schema definitions
│   ├── constants.py            # Business constants
│   └── transformations/        # Business logic JSONs
│
├── src/                        # Core pipeline
│   ├── reader.py               # Bronze/Silver readers
│   ├── ptf_mvt_run.py          # PTF_MVT orchestrator
│   ├── capitaux_run.py         # Capitaux orchestrator
│   ├── emissions_run.py        # Emissions orchestrator
│   └── processors/             # ETL processors
│
├── utils/                      # Utilities
│   ├── loaders/                # Config loaders
│   ├── logger.py               # Structured logging
│   ├── helpers.py              # Date/path helpers
│   └── transformations/        # Transform functions
│
├── notebooks/                  # Testing notebooks
│   └── ptf_mvt/                # 11 component tests
│
├── docs/                       # Documentation
│   ├── workflows/              # Pipeline workflows
│   └── configs/                # Config documentation
│
└── main.py                     # Entry point
```

---

## Documentation

| Document | Description |
|----------|-------------|
| [PTF_MVT Workflow](docs/workflows/PTF_MVT_Workflow.md) | Portfolio movements |
| [Capitaux Workflow](docs/workflows/Capitaux_Workflow.md) | Capital extraction |
| [Emissions Workflow](docs/workflows/Emissions_Workflow.md) | Premium emissions |
| [Configuration Guide](docs/configs/Configuration_Guide.md) | Config reference |
| [Data Catalog](docs/configs/Data_Catalog.md) | Input/output tables |

---

## Configuration

| File | Purpose |
|------|---------|
| `config.yml` | Paths, Spark, logging |
| `reading_config.json` | File patterns, schemas |
| `schemas.py` | PySpark type definitions |
| `constants.py` | Business constants |
| `*_transformations.json` | Business rules |

---

## Prerequisites

- Python 3.8+
- PySpark 3.x
- Azure Data Lake access (ABFSS)