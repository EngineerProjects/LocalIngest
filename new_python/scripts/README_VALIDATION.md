# 🔍 SAS vs Python Validation Guide

Simple guide to validate that Python outputs match SAS outputs.

---

## 📋 **What You Need**

1. **SAS aggregated files** (4 CSV files from your SAS queries)
2. **Python pipeline outputs** in the datalake gold layer
3. Python environment with pandas, numpy, openpyxl

---

## 🚀 **Step 1: Create Python Aggregations**

Aggregate Python outputs (same way you did in SAS):

```bash
python scripts/aggregate_python_outputs.py \
  --vision 202509 \
  --gold-path gs://your-datalake-bucket/gold \
  --output-dir ./python_aggregations
```

This creates 4 CSV files in `./python_aggregations/`

---

## ✅ **Step 2: Compare SAS vs Python**

Compare the files:

```bash
python scripts/validate_outputs.py \
  --vision 202509 \
  --sas-dir ./sas_outputs \
  --python-dir ./python_aggregations
```

---

## 📊 **What You'll See**

```
✅ PASS MVT_PTF
  SAS rows:    45
  Python rows: 45
  Matching:    45

✅ PASS CAPITAUX
  SAS rows:    45
  Python rows: 45
  Matching:    45

✅ ALL VALIDATIONS PASSED - Perfect parity!
```

Or if there are differences:

```
❌ FAIL MVT_PTF
  ⚠️  Mismatched columns: primes_ptf, nbafn
  📈 Max difference: 1250.50

  Top 10 differences:
  cdprod  column      sas_value  python_value  difference
  01234   primes_ptf  100000.50  98750.00      1250.50
```

---

## 📄 **Detailed Report**

A detailed Excel report is automatically saved: `validation_report.xlsx`

It contains:
- Summary sheet (all files)
- Individual sheets with all differences

---

## ⚙️ **Options**

### Change tolerance (default: 0.01)

```bash
python scripts/validate_outputs.py \
  --vision 202509 \
  --sas-dir ./sas_outputs \
  --python-dir ./python_aggregations \
  --tolerance 0.1
```

### Save report to different location

```bash
python scripts/validate_outputs.py \
  --vision 202509 \
  --sas-dir ./sas_outputs \
  --python-dir ./python_aggregations \
  --output-report ./my_report.xlsx
```

---

## 🎯 **That's It!**

Two simple steps:
1. Create Python aggregations
2. Compare with SAS

If all ✅ PASS → Perfect migration! 🎉
