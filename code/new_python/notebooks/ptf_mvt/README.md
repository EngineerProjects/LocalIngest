# PTF_MVT Testing Notebooks

## Overview

This directory contains Jupyter notebooks for testing each component of the PTF_MVT (Portfolio Movements) pipeline before running the full `python main.py --vision 202509` command.

## Testing Strategy

- **No assertions**: Focus on visual inspection and understanding data behavior
- **Real data**: Use actual datalake data when available (202509 vision)
- **Temporary data**: Create mock data for missing files
- **Config-driven**: Test with real configurations from JSON/Python config files
- **Incremental**: Build confidence step-by-step before full pipeline execution

## Notebook Structure

| #   | Notebook                                     | Purpose                                                          | Dependencies      |
| --- | -------------------------------------------- | ---------------------------------------------------------------- | ----------------- |
| 01  | `01_bronze_file_reading.ipynb`               | Bronze file reading, schema application, data quality            | None (data only)  |
| 02  | `02_business_filters_testing.ipynb`          | Business filters (AZ/AZEC), exclusions, market/segment filtering | Notebook 01       |
| 03  | `03_helper_functions_testing.ipynb`          | Date calculations, vision parsing, path building                 | None (standalone) |
| 04  | `04_capital_extraction_testing.ipynb`        | Capital extraction (SMP, LCI, PERTE_EXP, RISQUE_DIRECT)          | Notebooks 01-02   |
| 05  | `05_movement_calculations_testing.ipynb`     | Movement indicators (NBAFN, NBRES, NBRPT, NBRPC, NBPTF)          | Notebooks 01-03   |
| 06  | `06_column_transformations_testing.ipynb`    | Column operations, computed columns, conditional transforms      | Notebooks 01-02   |
| 07  | `07_coassurance_az_transforms_testing.ipynb` | Coassurance logic, revision criteria, AZ transform steps         | Notebooks 01-06   |
| 08  | `08_full_az_processor_testing.ipynb`         | Complete AZ processor end-to-end (read→transform→write)          | All previous      |

## Execution Order

Run notebooks in sequence (01 → 08) to build understanding incrementally:

```bash
# Start with data reading
jupyter notebook 01_bronze_file_reading.ipynb

# Test filters
jupyter notebook 02_business_filters_testing.ipynb

# Test helpers (standalone)
jupyter notebook 03_helper_functions_testing.ipynb

# Test business logic components
jupyter notebook 04_capital_extraction_testing.ipynb
jupyter notebook 05_movement_calculations_testing.ipynb
jupyter notebook 06_column_transformations_testing.ipynb

# Test full transformations
jupyter notebook 07_coassurance_az_transforms_testing.ipynb
jupyter notebook 08_full_az_processor_testing.ipynb
```

## Key Components Tested

### Bronze Layer
- File reading (ipf, ipfm99_az, polic_cu, capitxcu, reference data)
- Schema enforcement
- Column lowercase conversion
- Read filters

### Business Filters
- AZ: market=6, segment=2, policy status, intermediaries, NAF codes
- AZEC: excluded intermediaries/policies, migration logic

### Helper Functions
- Vision validation
- Date range computation (DTFIN, DTDEB_AN, dtdebn, dtfinmn, dtfinmm1)
- Path building

### Business Logic
- **Capital Extraction**: SMP, LCI, PERTE_EXP, RISQUE_DIRECT from mtcapi1-14/lbcapi1-14
- **Movement Calculations**: NBAFN (new), NBRES (terminations), NBRPT/NBRPC (replacements), NBPTF (portfolio)
- **Exposure Calculations**: expo_ytd, expo_gli
- **Coassurance**: top_coass, coass type, partcie, top_revisable
- **Transformations**: primeto, mtca_, top_lta, top_aop, cotis_100

### Full Pipeline
- AZ Processor (10 transformation steps)
- AZEC Processor (11 transformation steps)  
- Consolidation (harmonization + union)

## Data Sources

### Available Data (202509)
- Bronze monthly: IMS_INFP_IIA0P6_*IPF*.csv.gz, ird_risk_*.csv, POLIC_CU.csv, CAPITXCU.csv
- Bronze reference: constru.csv, cproduit.csv, garantcu.csv, prdpfa*.csv, etc.

### Missing Data
- Create temporary mock data in notebooks
- Replace with real data when available

## Configuration Files Used

- `config/config.yml`: Main pipeline configuration
- `config/reading_config.json`: File group definitions
- `config/schemas.py`: PySpark schemas
- `config/constants.py`: Business constants
- `config/variables.py`: Column configurations (legacy wrapper for JSON)
- `config/transformations/az_transformations.json`: AZ transformation configs
- `config/transformations/azec_transformations.json`: AZEC transformation configs
- `config/transformations/business_rules.json`: Coassurance, filters, transform steps
- `config/transformations/consolidation_mappings.json`: Harmonization mappings

## Success Criteria

Before running `main.py`:
- ✓ All bronze files read successfully
- ✓ Business filters reduce data appropriately
- ✓ Date ranges computed correctly
- ✓ Capitals extracted matching expected patterns
- ✓ Movements calculated with correct logic
- ✓ Transformations applied without errors
- ✓ End-to-end processor produces expected output

## Notes

- Notebooks use actual configurations from the codebase
- Visual inspection preferred over automated assertions
- Focus on understanding data flow and transformations
- Iterative refinement: fix issues before moving to next notebook

## Next Steps After Testing

Once all notebooks pass:
1. Run full pipeline: `python main.py --vision 202509`
2. Review logs in `logs/pipeline_202509.log`
3. Inspect outputs in silver/gold layers
4. Compare with SAS legacy outputs (if available)
5. Document any discrepancies
