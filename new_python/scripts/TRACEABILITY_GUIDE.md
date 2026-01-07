# Product Traceability - How to Use

## Purpose
Track a specific product (CDPROD) through the entire PTF_MVT pipeline to verify:
- The product exists at each stage
- Data transformations are correct
- No unexpected disappearances
- Values match expectations

## Setup

### 1. Choose a Product to Trace

Look in SAS output to find an interesting product:
```bash
# Check SAS output
head -20 output/mvt_ptf202509.csv | column -t -s','

# Look for cdprod column and pick a value
# Good candidates:
# - Common products that should have many rows
# - Special products (01099, DO0, TRC, etc.)
# - Products that appear in both AZ and AZEC
```

### 2. Run the Trace Script

```bash
# On the Spark cluster (where PySpark is available):
python3 scripts/trace_product.py <CDPROD> [VISION]

# Examples:
python3 scripts/trace_product.py 01099         # Trace product 01099 (vision 202509)
python3 scripts/trace_product.py DO0 202509    # Trace DO0 for vision 202509
python3 scripts/trace_product.py TRC           # Trace TRC product
```

## What the Script Does

### Stage 1: Bronze AZ (Raw Data)
- Checks PTF16.csv (Agent - Pole 1)
- Checks PTF36.csv (Courtage - Pole 3)
- Shows how many rows for this product
- Displays sample data

### Stage 2: Bronze AZEC (Raw Data)
- Checks POLIC_CU.csv
- Shows rows with this produit code
- Displays sample data

### Stage 3: Silver AZ (After Transformation)
- Checks if product survived business filters (cmarch='6', cseg='2', etc.)
- Shows transformed data
- Alerts if deduplication removed rows

### Stage 4: Silver AZEC (After Transformation)
- Checks if product survived business filters
- Checks construction market filter (cmarch='6')
- Shows transformed data

### Stage 5: Gold (Final Consolidated)
- Verifies product in final output
- Shows distribution by DIRCOM (AZ vs AZEC)
- Shows distribution by CDPOLE (Pole 1 vs Pole 3)
- Verifies union is correct

## Output Example

```
🔬🔬🔬🔬🔬🔬🔬🔬
PRODUCT TRACEABILITY ANALYSIS
Product Code: 01099
🔬🔬🔬🔬🔬🔬🔬🔬

================================================================================
  STAGE 1: BRONZE - AZ (PTF16 + PTF36)
================================================================================
✅ PTF16 (Pole 1): Product FOUND (45 rows)

📊 Data at PTF16 (Bronze AZ Pole 1):
cdprod  | nopol      | cmarch | csegt | mtprprto  | ...
01099   | 123456789  | 6      | 2     | 1250.50   | ...

✅ PTF36 (Pole 3): Product FOUND (23 rows)

================================================================================
  STAGE 3: SILVER - AZ (After Transformation)
================================================================================
✅ Silver AZ: Product FOUND (65 rows)

⚠️  DEDUPLICATION: 68 → 65 rows

================================================================================
  STAGE 5: GOLD - Consolidated (AZ + AZEC)
================================================================================
✅ Gold: Product FOUND (150 rows)

📊 Distribution by DIRCOM:
dircom | count
AZ     | 65
AZEC   | 85

================================================================================
  TRACEABILITY SUMMARY
================================================================================
🔍 Product: 01099
📊 Journey:
  Bronze AZ (PTF16+PTF36)       : ✅ FOUND (68 rows)
  Bronze AZEC (POLIC_CU)        : ✅ FOUND (92 rows)
  Silver AZ                     : ✅ FOUND (65 rows)
  Silver AZEC                   : ✅ FOUND (85 rows)
  Gold (Final)                  : ✅ FOUND (150 rows)

🚨 Alerts:
  ✅ No disappearances detected
```

## Use Cases

### 1. Verify Business Filters Work
Trace a product that should be filtered out (e.g., cmarch != '6'):
- Should appear in Bronze
- Should disappear in Silver

### 2. Debug Missing Data
If a product is in SAS but not in Python:
- See at which stage it disappears
- Check the filter/transformation that removed it

### 3. Compare Transformations
Trace a product and compare values at each stage with SAS:
- Bronze values should match raw SAS input
- Silver values should match SAS after transformations
- Gold values should match SAS final output

### 4. Verify Deduplication
Trace a product that has duplicates:
- See how many duplicates in Bronze
- Verify only 1 remains in Silver (after dedupby nopol)

## Alerts to Watch For

### ⚠️ Product Disappeared
```
🚨 Alerts:
  ⚠️  AZ: Product disappeared between Bronze and Silver!
```
**Action**: Check business_filters - product was filtered out

### ⚠️ Unexpected Row Count
```
⚠️  DEDUPLICATION: 100 → 50 rows
```
**Action**: Check if deduplication is too aggressive

### ⚠️ Union Issue
```
⚠️  UNION ISSUE: Expected 150 rows but found 148
```
**Action**: Check consolidation union logic

## Tips

1. **Start with common products** (products that appear frequently)
2. **Compare with SAS** output for the same product
3. **Use vertical display** for detailed column inspection
4. **Check null values** - might cause unexpected filtering
5. **Verify cmarch** - construction market filter is critical

## Next Steps

After tracing a product:
1. Compare values with SAS at each stage
2. If disappeared, check the filter that removed it
3. If values differ, check the transformation logic
4. Document any differences found
