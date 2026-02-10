# Configuration Guide

## Overview

The pipeline uses a **configuration-driven** architecture with all business logic externalized from code.

---

## Configuration Files

| File                            | Location                  | Purpose                                |
| ------------------------------- | ------------------------- | -------------------------------------- |
| **config.yml**                  | `config/`                 | Paths, Spark settings, logging         |
| **reading_config.json**         | `config/`                 | File patterns and read options         |
| **schemas.py**                  | `config/`                 | PySpark schema definitions             |
| **constants.py**                | `config/`                 | Business constants and exclusion lists |
| **az_transformations.json**     | `config/transformations/` | AZ channel logic                       |
| **azec_transformations.json**   | `config/transformations/` | AZEC channel logic                     |
| **business_rules.json**         | `config/transformations/` | Shared business rules                  |
| **emissions_config.json**       | `config/transformations/` | Emissions filters and mappings         |
| **consolidation_mappings.json** | `config/transformations/` | AZ/AZEC schema harmonization           |

---

## 1. Global Settings (config.yml)

### Key Sections

| Section              | Purpose                                 |
| -------------------- | --------------------------------------- |
| `datalake.base_path` | Root path for bronze/silver/gold layers |
| `output.format`      | Output format (delta, parquet)          |
| `components`         | Enable/disable pipeline components      |
| `spark.config`       | Spark session configuration             |
| `logging`            | Log level and file location             |
| `vision.validation`  | Year range validation                   |

### Output Configuration

| Key                  | Default     | Description                               |
| -------------------- | ----------- | ----------------------------------------- |
| `output.format`      | `delta`     | Output format (delta for ACID guarantees) |
| `output.mode`        | `overwrite` | Write mode (overwrite, append)            |
| `output.compression` | `snappy`    | Compression codec                         |
| `output.clean`       | `true`      | Clean existing data before write          |

---

## 2. File Reading (reading_config.json)

### File Group Structure

| Field                   | Description                       |
| ----------------------- | --------------------------------- |
| `description`           | Human-readable description        |
| `file_patterns`         | Glob patterns for file matching   |
| `schema`                | Reference to schema in schemas.py |
| `read_options.format`   | csv, parquet                      |
| `read_options.sep`      | Column separator                  |
| `read_options.header`   | Has header row                    |
| `read_options.encoding` | ISO-8859-15, UTF-8                |
| `location_type`         | monthly or reference              |

### Location Types

| Type          | Path Pattern          |
| ------------- | --------------------- |
| **monthly**   | `bronze/{YYYY}/{MM}/` |
| **reference** | `bronze/ref/`         |

### Current File Groups (45 total)

- IMS files: `ipf`, `ipfm99_az`, `ipfspe_*`
- OneBI: `rf_fr1_prm_dtl_midcorp_m`
- AZEC: `polic_cu_azec`, `capitxcu_azec`, `incendcu_azec`, etc.
- IRD Risk: `ird_risk_q45`, `ird_risk_q46`, `ird_risk_qan`
- Reference: `segmentprdt`, `ptgst`, `cproduit`, `prdcap`, etc.
- New tables: `ref_mig_azec_vs_ims`, `indices`, `mapping_isic_*`, `ref_isic`

---

## 3. Schemas (schemas.py)

Explicit PySpark schemas for type safety.

### Schema Registry

Maps file group names to StructType schemas:
- `ipf` → IPF_AZ_SCHEMA
- `capitxcu_azec` → CAPITXCU_SCHEMA
- etc.

### Benefits

- ✅ Type safety and validation
- ✅ Better performance (no schema inference)
- ✅ Self-documenting data dictionary

---

## 4. Constants (constants.py)

### Business Constants

| Constant | Values                             |
| -------- | ---------------------------------- |
| DIRCOM   | AZ="AZ", AZEC="AZEC" (string)      |
| CDPOLE   | Agent="1", Courtage="3" (string)   |
| CMARCH   | Construction="6"                   |
| CSEG     | Segment="2" (Construction segment) |

### Exclusion Lists

| List              | Count | Used In       |
| ----------------- | ----- | ------------- |
| NOINT exclusions  | 20    | AZ filters    |
| AZEC intermediary | 2     | AZEC filters  |
| Excluded products | 1     | All pipelines |

---

## 5. Transformation JSONs

### az_transformations.json

| Section              | Purpose                               |
| -------------------- | ------------------------------------- |
| `column_selection`   | Passthrough, rename, computed columns |
| `business_filters`   | Market, segment, status filters       |
| `capital_extraction` | Keyword-based capital extraction      |
| `movements`          | Date column mappings                  |

### business_rules.json

| Section              | Purpose                    |
| -------------------- | -------------------------- |
| `coassurance_config` | COASS type determination   |
| `az_transform_steps` | Business rule calculations |
| `gestsit_rules`      | Status updates             |

### azec_transformations.json

| Section              | Purpose                       |
| -------------------- | ----------------------------- |
| `column_selection`   | AZEC column configuration     |
| `business_filters`   | AZEC-specific filters         |
| `migration_handling` | Vision > 202009 logic         |
| `movement_products`  | 47-product list for movements |

### emissions_config.json

| Section                   | Purpose               |
| ------------------------- | --------------------- |
| `excluded_intermediaries` | 15 intermediary codes |
| `excluded_guarantees`     | 4 guarantee codes     |
| `channel_mapping`         | cd_niv_2_stc → CDPOLE |

---

## Adding New Configuration

### New File Group

1. Add entry to `reading_config.json`
2. Add schema to `schemas.py`
3. Register in SCHEMA_REGISTRY

### New Business Rule

1. Add to appropriate transformation JSON
2. Test with sample data
3. Document in this guide

---

## Troubleshooting

| Issue                       | Solution                                          |
| --------------------------- | ------------------------------------------------- |
| Schema validation fails     | Check column names match exactly (case-sensitive) |
| Filter not applied          | Verify column exists before filter step           |
| File not found              | Check file_pattern glob matches filenames         |
| Transformation returns null | Add default values                                |

---

**Last Updated**: 2026-02-06  
**Version**: 1.1
