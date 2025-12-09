# Utils Testing Notebooks

This directory contains test notebooks for **individual helper functions and transformations** used across the pipeline.

## Purpose

These notebooks test utility components **in isolation** before they're used in the full pipeline. They help confirm that:
- ✅ Helper functions produce correct outputs
- ✅ Transformations apply correctly
- ✅ Validators catch data quality issues
- ✅ Configuration files load and parse properly

## Testing Philosophy

All notebooks use **visual inspection** instead of automated assertions:
- You review the outputs yourself
- Flexible evaluation of "correctness"
- Easier to understand data behavior
- Catches subtle issues that assertions might miss

## Notebooks

### 01_helpers_testing.ipynb
**Tests:** `utils/helpers.py`

Functions tested:
- `validate_vision()` - Vision format validation (YYYYMM)
- `extract_year_month()` / `extract_year_month_int()` - Year/month extraction
- `compute_date_ranges()` - Date range computation (DTFIN, DTDEB_AN, etc.)
- `build_layer_path()` - Datalake path construction
- `build_log_filename()` - Log filename generation

**Coverage:** Vision parsing, date computations, path building

---

### 02_generic_transforms_business_logic_testing.ipynb
**Tests:** `utils/transformations/`

Functions tested:
- **Generic transforms:**
  - `apply_conditional_transform()` - when/otherwise logic from config
  - `apply_business_filters()` - DataFrame filtering
  - `apply_transformations()` - Series of transformations
- **Column operations:**
  - `lowercase_all_columns()` - Column standardization
  - `apply_column_config()` - Passthrough/rename/computed columns
- **Business logic:**
  - `extract_capitals()` - SMP/LCI extraction from label/amount fields
  - `calculate_movements()` - AFN, RES, RPT, RPC, NBPTF calculations
  - `calculate_exposures()` - expo_ytd and expo_gli calculations

**Coverage:** Configuration-driven transformations, capital extraction, movements, exposures

---

### 03_config_testing.ipynb (Optional - Can be created later)
**Tests:** `config/` directory

For configuration and data validation, you can create custom notebooks to:
- Load and validate JSON configs (business_rules, transformations)
- Verify constants (`DIRCOM`, market codes, exclusions)
- Validate capital extraction configs
- Validate business filter configs
- Perform data quality checks on your datasets

**Note:** Validation logic should be implemented in your own analysis notebooks as needed for better flexibility and interactive analysis.

---

## How to Use

### Prerequisites
```bash
# Ensure PySpark is available
pip install pyspark

# From project root:
cd notebooks/utils
jupyter notebook
```

### Running Tests

1. **Open a notebook** in Jupyter
2. **Run all cells** (Kernel → Restart & Run All)
3. **Review outputs** visually
4. **Check verifications** - Each test section has verification messages like:
   - `✓ Correct: year='2025' and month='09'`
   - `✗ INCORRECT`

### What to Look For

**Good outcomes:**
- ✓ marks on verification checks
- Expected values in output tables
- Consistent results across test cases
- No exceptions or errors

**Issues to investigate:**
- ✗ marks on verification checks
- Unexpected values in outputs
- Exceptions or stack traces
- NULL values where data should exist

---

## Relationship to Pipeline Tests

| Test Type | Location | Purpose |
|-----------|----------|---------|
| **Utils tests (this directory)** | `notebooks/utils/` | Test individual helper functions and transformations in isolation |
| **Column discovery** | `notebooks/00_column_discovery.ipynb` | Validate input data columns against SAS requirements |
| **PTF_MVT tests** | `notebooks/ptf_mvt/` | Test end-to-end processor execution with real data |

**Testing flow:**
1. ✅ **Utils tests** (this directory) → Confirm helpers work correctly
2. ✅ **Column discovery** → Confirm input data is available
3. ✅ **PTF_MVT tests** → Confirm full pipeline execution

---

## Adding New Tests

When adding new helper functions or transformations:

1. **Create test cells** in the relevant notebook
2. **Use visual inspection pattern:**
   ```python
   # Test description
   result = my_function(test_input)
   print(f"Result: {result}")
   print(f"  ✓ Correct" if result == expected else f"  ✗ Wrong: expected {expected}")
   ```
3. **Include multiple test cases:**
   - Valid inputs
   - Edge cases
   - Invalid inputs (if applicable)
4. **Show intermediate outputs** when helpful

---

## Configuration Files Tested

The notebooks load and validate these configuration files:

- `config/transformations/business_rules.json` - Coassurance, filters, business logic
- `config/transformations/az_transformations.json` - AZ column mappings and transformations
- `config/transformations/azec_transformations.json` - AZEC transformations and product lists
- `config/transformations/consolidation_mappings.json` - Schema harmonization
- `config/constants.py` - Business constants and exclusion lists

---

## Summary

✅ **Test helpers** individually before using in pipeline
✅ **Visual inspection** for flexibility and understanding
✅ **Two focused notebooks** covering helpers and transformations
✅ **Clear verification messages** for each test
✅ **Create custom validation notebooks** as needed for your analysis
