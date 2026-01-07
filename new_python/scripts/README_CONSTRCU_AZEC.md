# CONSTRCU_AZEC Preprocessing Script

## Purpose

This script generates the `CONSTRCU_AZEC` reference table which is required by the AZEC pipeline.

**CONSTRCU_AZEC** is NOT a raw data file - it's a **preprocessed table** created from CONSTRCU by applying business logic.

---

## What It Does

Replicates SAS logic from `REF_segmentation_azec.sas` (lines 226-343):

1. **Reads CONSTRCU** (raw construction site data with PRODUIT)
2. **Joins with LOB** reference to get:
   - `CDPROD` (product code from LOB)
   - `SEGMENT` (segment classification)
3. **Calculates TYPE_PRODUIT** based on business rules:
   - `TRC` for "TOUS RISQUES CHANTIERS"
   - `DO` for "DOMMAGES OUVRAGES"  
   - `Entreprises` for product RCC
   - `Artisans` for RBA/RCD with typmarc1='01'
   - `Autres` otherwise
4. **Outputs 4 columns**: POLICE, CDPROD, SEGMENT, TYPE_PRODUIT

---

## Usage

### Basic Usage (Reference Data):
```bash
python scripts/create_constrcu_azec.py
```

### For Specific Vision:
```bash
python scripts/create_constrcu_azec.py --vision 202509
```

---

## When to Run

**Run this script BEFORE running the main pipeline** if:
- CONSTRCU_AZEC doesn't exist yet
- CONSTRCU source data has been updated
- You're running the pipeline for a new vision period

---

## Input Files Required

The script needs these tables in `bronze/ref/`:
- `constrcu.csv` - Raw CONSTRCU data
- `lob.csv` - Product segmentation reference

---

## Output

Creates: `bronze/ref/constrcu_azec_{vision}/` (Parquet format)

With columns:
- `police` - Policy number
- `cdprod` - Product code (from LOB)
- `segment` - Segment classification
- `type_produit` - Product type (TRC, DO, Artisans, Entreprises, Autres)

---

## Integration with Pipeline

Once generated, the AZEC processor will automatically use this table:

```python
# In azec_processor.py
constrcu_ref = reader.read_file_group('constrcu_azec', vision='ref')
```

---

## Troubleshooting

### Error: "Failed to read CONSTRCU"
→ Make sure `constrcu.csv` exists in `bronze/ref/`

### Error: "Failed to read LOB"
→ Make sure `lob.csv` exists in `bronze/ref/`

### Empty output
→ Check that LOB has records with `cmarch='6'` (construction market)

---

## SAS Correspondence

| SAS Script | Python Script |
|------------|---------------|
| REF_segmentation_azec.sas L226-231 | `_read_constrcu()` |
| REF_segmentation_azec.sas L227 (%SEGMENTA) | `_enrich_with_lob()` |
| REF_segmentation_azec.sas L324-335 | `_calculate_type_produit()` |
| REF_segmentation_azec.sas L341-343 | `generate()` final select |
