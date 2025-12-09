# Emissions Workflow

## Purpose

Processes One BI premium data to calculate:
- Total premiums (all years)
- Current year premiums
- Commissions by channel

---

## Pipeline Overview

```mermaid
graph LR
    Bronze[Bronze Layer] --> Transform[Transform]
    Transform --> Gold[Gold Layer]
```

**Note:** Emissions writes directly to Gold (no Silver intermediate)

**Output:** Two aggregation levels - by guarantee and by policy

---

## Input Sources

| Source | File Pattern | Layer | Description |
|--------|-------------|-------|-------------|
| OneBI Premium | `rf_fr1_prm_dtl_midcorp_m_*.csv` | Bronze/monthly | Premium transactions |
| SEGMPRDT | `segmentprdt_*.csv` | Bronze/ref | Product segmentation |

---

## Business Filters

| Filter | Value | Description |
|--------|-------|-------------|
| cd_marche | 6 | Construction market |
| dt_cpta_cts | ≤ vision | Accounting date cutoff |
| cd_int_stc | Exclusion list | 15 excluded intermediaries |
| cd_prd_prm | ≠ 01073 | Excluded product |
| cd_gar_prospctiv | Exclusion list | 4 excluded guarantees |
| cd_cat_min | ≠ 080/090/095/099 | Excluded categories |

---

## Key Calculations

### Channel Assignment (CDPOLE)

| cd_niv_2_stc | CDPOLE | Channel |
|--------------|--------|---------|
| DCAG | 1 | Agent |
| DCPS | 1 | Agent |
| DIGITAL | 1 | Agent |
| BROKDIV | 3 | Courtage |

### Year Classification (EXERCICE)

| Condition | EXERCICE | Meaning |
|-----------|----------|---------|
| nu_ex_ratt ≥ vision year | 'cou' | Current year |
| nu_ex_ratt < vision year | 'ant' | Prior year |

### Premium Metrics

| Metric | Description |
|--------|-------------|
| **primes_x** | Sum of mt_ht_cts (all years) |
| **primes_n** | Sum where exercice = 'cou' |
| **mtcom_x** | Sum of mt_cms_cts |

---

## Output Datasets

### Gold Layer (Final)

| # | Dataset | Granularity | Rows | Description |
|---|---------|-------------|------|-------------|
| 1 | `primes_emises_{vision}_pol_garp` | Policy + Guarantee | ~12K | Detailed by guarantee |
| 2 | `primes_emises_{vision}_pol` | Policy | ~8K | Aggregated by policy |

---

## Output Schema (primes_emises_pol_garp)

### Identifiers
- `vision` - Processing vision (YYYYMM)
- `nopol` - Policy number
- `cdprod` - Product code
- `noint` - Intermediary code
- `cgarp` - Guarantee code (chars 3-5 from cd_gar_prospctiv)

### Channel
- `dircom` - Commercial direction ('AZ')
- `cdpole` - Distribution channel (1=Agent, 3=Courtage)

### Segmentation
- `cmarch` - Market code (6 = Construction)
- `cseg`, `cssseg` - Segment codes
- `cd_cat_min` - Minimum category

### Financial
- `primes_x` - Total premiums (all years)
- `primes_n` - Current year premiums
- `mtcom_x` - Total commissions

---

## Output Schema (primes_emises_pol)

Same as pol_garp but **without**:
- `cgarp` (guarantee code)
- `cd_cat_min` (category)

Premiums aggregated across all guarantees for each policy.
