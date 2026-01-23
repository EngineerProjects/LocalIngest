# Deduplication and Join Analysis

## Finding 1: Deduplication Logic ✅ **CORRECT**

### SAS Implementation
```sas
-- Step 1: Join with segmentation and PT_GEST
select a.*, b.segment as segment2, b.type_produit_2, c.Upper_MID
from MVT_CONST_PTF a
left join segment b on a.cdprod = b.cprod and a.CDPOLE = b.reseau
left join TABLE_PT_GEST c on a.PTGST = c.PTGST
order by nopol, cdsitp;  -- Orders data

-- Step 2: Deduplicate
proc sort data=MVT_CONST_PTF nodupkey;
    by nopol;  -- Keeps FIRST occurrence after ordering by (nopol, cdsitp)
run;
```

### PySpark Implementation
```python
# az_processor.py L674-683, L363-364
df = df.join(
    broadcast(df_segment...),
    on=['cdprod', 'cdpole'],
    how='left'
)
# ... PT_GEST join ...
df = df.orderBy("nopol", "cdsitp").dropDuplicates(["nopol"])
```

**Status**: ✅ Logic is **equivalent**
- Both order by `(nopol, cdsitp)` before deduplication
- Both keep **first** occurrence (SAS `nodupkey`, Spark `dropDuplicates` after orderBy)
- Both deduplicate on `nopol` only

---

## Finding 2: Segment Table Preparation - POTENTIAL ISSUE ⚠️

### SAS Segment Table (L454-472)
```sas
DATA Segment1;  -- From PRDPFA1
    SET SEGMprdt.PRDPFA1 (KEEP = cmarch...);
    WHERE cmarch IN ('6');
    reseau = '1';
    KEEP CPROD lprod cseg lseg2 cssseg lssseg2 cmarch lmarch2 reseau;
RUN;

PROC SORT DATA = Segment1 NODUPKEY;
    BY CPROD;  -- Deduplicate by CPROD only (within reseau='1')
RUN;

-- Same for Segment3 (reseau='3')

DATA Segment;
    SET Segment1 Segment3;  -- Union
RUN;

PROC SORT DATA = Segment;
    BY CPROD;  -- Sort by CPROD (NOT by reseau)
RUN;
```

**CRITICAL**: SAS deduplicates Segment1 and Segment3 **separately by CPROD**,  then unions them without re-deduplicating!

### PySpark Implementation (L665)
```python
df_segment = df_segment.dropDuplicates(['reseau', 'cprod'])
```

**POTENTIAL ISSUE**: PySpark deduplicates by `['reseau', 'cprod']` AFTER the union, which is **stricter** than SAS!

**Impact**: This should NOT cause duplicates, but could affect which row is kept if there are duplicates within Products across reseau. However, this shouldn't happen since:
- Segment1 is filtered `reseau='1'` 
- Segment3 is filtered `reseau='3'`
- They shouldn't have overlapping (reseau, cprod) combinations

---

## Finding 3: Reference Join Cardinality - Need to Verify

### Potential Duplication Sources
1. **PRDCAP join** (L651-658): Uses LEFT join + dropDuplicates(['cprod'])
   - If PRDCAP has duplicates for same cprod → could multiply rows before dropDuplicates
   
2. **CPRODUIT join** (L635-640): Uses LEFT join
   - If CPRODUIT has duplicates → could multiply rows
   
3. **TABLE_PT_GEST join** (L699-709): Uses `safe_reference_join`
   - Should be safe (broadcast + left join)

### SAS Approach
```sas
PROC SORT DATA = PRDCAPa NODUPKEY;
    BY CPROD;  -- Deduplicates BEFORE join
RUN;

PROC SORT DATA = Cproduit NODUPKEY;
    BY CPROD;  -- Deduplicates BEFORE join
RUN;
```

**SAS deduplicates reference tables BEFORE joining!**

### PySpark Approach (az_processor.py)
```python
# L644-658: PRDCAP
df_prdcap = df_prdcap.select(...).dropDuplicates(['cprod'])  # Deduplicates AFTER select
df_segment = df_segment.join(df_prdcap, on='cprod', how='left')

# L628-633: CPRODUIT
df_cproduit_enrichment = df_cproduit.select(...)  # NO dropDuplicates!
df_segment = df_segment.join(df_cproduit_enrichment, on='cprod', how='left')
```

---

## ⚠️ **CRITICAL FINDING: Missing CPRODUIT Deduplication!**

**SAS** (L417-419):
```sas
PROC SORT DATA = Cproduit NODUPKEY;
    BY CPROD;
RUN;
```

**PySpark** (L628-640):
```python
df_cproduit_enrichment = df_cproduit.select(
    col('cprod'),
    col('Type_Produit_2').alias('type_produit_2'),
    col('segment_from_cproduit'),
    col('Segment_3').alias('segment_3')
)

df_segment = df_segment.join(
    df_cproduit_enrichment,
    on='cprod',
    how='left'
)
```

**NO `dropDuplicates(['cprod'])` on CPRODUIT before join!**

### Impact
If CPRODUIT reference table has duplicate `cprod` values, the LEFT join will create a **cartesian product**, multiplying rows!

**Example**:
```
df_segment: 
  cprod='00526', reseau='1'  → 1 row

df_cproduit (with duplicates):
  cprod='00526', type_produit_2='A'  → Row 1
  cprod='00526', type_produit_2='B'  → Row 2

After LEFT join:
  cprod='00526', reseau='1', type_produit_2='A'
  cprod='00526', reseau='1', type_produit_2='B'
→ 2 rows instead of 1!
```

This would explain the systematic policy inflation!

---

## Recommendation: Fix CPRODUIT Join

### Change Required in az_processor.py (Line 628-640)

**Current (WRONG)**:
```python
df_cproduit_enrichment = df_cproduit.select(
    col('cprod'),
    col('Type_Produit_2').alias('type_produit_2'),
    col('segment_from_cproduit'),
    col('Segment_3').alias('segment_3')
)

df_segment = df_segment.join(
    df_cproduit_enrichment,
    on='cprod',
    how='left'
)
```

**Fixed (ADD dropDuplicates)**:
```python
df_cproduit_enrichment = df_cproduit.select(
    col('cprod'),
    col('Type_Produit_2').alias('type_produit_2'),
    col('segment_from_cproduit'),
    col('Segment_3').alias('segment_3')
).dropDuplicates(['cprod'])  # ← ADD THIS LINE (SAS L417-419)

df_segment = df_segment.join(
    df_cproduit_enrichment,
    on='cprod',
    how='left'
)
```

---

## Verification Steps

### 1. Check CPRODUIT for Duplicates
```python
from pyspark.sql.functions as F

df_cproduit = reader.read_file_group('cproduit', 'ref')

# Count total vs distinct
total = df_cproduit.count()
distinct = df_cproduit.select('cprod').distinct().count()

print(f"CPRODUIT total rows: {total:,}")
print(f"CPRODUIT distinct cprod: {distinct:,}")
print(f"Duplicates: {total - distinct:,}")

if total != distinct:
    # Show duplicate products
    df_cproduit.groupBy('cprod').count() \
        .filter(F.col('count') > 1) \
        .orderBy(F.col('count').desc()) \
        .show(20)
```

### 2. Verify Impact on df_segment
```python
# Before CPRODUIT join
count_before = df_segment.count()

# After CPRODUIT join (WITHOUT dropDuplicates)
# ...join...
count_after = df_segment.count()

print(f"Before CPRODUIT join: {count_before:,}")
print(f"After CPRODUIT join: {count_after:,}")
print(f"Rows added: {count_after - count_before:,}")

# Should be 0! If > 0, CPRODUIT has duplicates
```

### 3. Test Final Policy Count After Fix
```python
# After fixing CPRODUIT join + final deduplication
total_policies = df_az.count()
distinct_nopol = df_az.select('nopol').distinct().count()

print(f"Final total: {total_policies:,}")
print(f"Final distinct nopol: {distinct_nopol:,}")

# These should be EQUAL after final dropDuplicates(['nopol'])
assert total_policies == distinct_nopol, "Duplicates exist after final dedup!"
```
