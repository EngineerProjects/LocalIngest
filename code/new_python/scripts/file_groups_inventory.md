# Complete List of File Groups Used in AZ, AZEC, and CONSOLIDATION Pipelines

## AZEC Processor File Groups

### Read in `read()` method:
- `polic_cu_azec` (vision=ref) - **CRITICAL: Fixed to yyyy-MM-dd**

### Read in `transform()` method:
1. `table_segmentation_azec_mml` (ref) - STEP 08
2. `ref_mig_azec_vs_ims` (ref) - STEP 03
3. `capitxcu_azec` (vision) - STEP 15
4. `ptgst_static` (ref) - Point de gestion
5. `incendcu_azec` (vision) - STEP 10 (NAF), STEP 13 (PE/RD/VI)
6. `mpacu_azec` (vision) - STEP 10 (NAF)
7. `rcentcu_azec` (vision) - STEP 10 (NAF), STEP 11 (Formules)
8. `risteccu_azec` (vision) - STEP 10 (NAF), STEP 11 (Formules)
9. `mulprocu_azec` (vision) - STEP 12 (CA)
10. `lob` (ref) - STEP 13 (CONSTRCU build)
11. `constrcu` (ref) - STEP 13 (CONSTRCU build), STEP 17 (site data)
12. `table_pt_gest` (vision) - STEP 17 (UPPER_MID)

**Total: 12 distinct file groups**

---

## AZ Processor File Groups

### Read in `read()` method:
- `ipf` (vision) - Main IPF data

### Read in `transform()` method:
1. `segmprdt_prdpfa1` (ref) - Segmentation
2. `segmprdt_prdpfa3` (ref) - Segmentation
3. `cproduit` (ref) - Product mapping
4. `prdcap` (ref) - Capital mapping

**Total: 5 distinct file groups**

---

## CONSOLIDATION Processor File Groups

### Read in `transform()` method:
1. `binsee_histo_note_risque` (ref) - Euler risk scores
2. `ipfm0024_1` (vision) - IPFSPE data
3. `ipfm0024_3` (vision) - IPFSPE data
4. `ipfm63_1` (vision) - IPFSPE data
5. `ipfm63_3` (vision) - IPFSPE data
6. `ipfm99_1` (vision) - IPFM99 data
7. `ipfm99_3` (vision) - IPFM99 data
8. `w6_basecli_inv` (ref) - Client base
9. `cliact14` (ref) - Client active data
10. `cliact3` (ref) - Client active data
11. `isic_lg` (ref) - ISIC codes
12. `do_dest` (vision) - Destination data

**Total: 12 distinct file groups**

---

## COMPLETE FILE GROUP LIST FOR AUDIT

```python
FILE_GROUPS_TO_AUDIT = [
    # AZEC (12)
    "polic_cu_azec",              # ✅ FIXED: yyyy-MM-dd
    "table_segmentation_azec_mml",
    "ref_mig_azec_vs_ims",
    "capitxcu_azec",
    "ptgst_static",
    "incendcu_azec",
    "mpacu_azec",
    "rcentcu_azec",
    "risteccu_azec",
    "mulprocu_azec",
    "lob",
    "constrcu",
    "table_pt_gest",
    
    # AZ (5)
    "ipf",
    "segmprdt_prdpfa1",
    "segmprdt_prdpfa3",
    "cproduit",
    "prdcap",
    
    # CONSOLIDATION (12)
    "binsee_histo_note_risque",
    "ipfm0024_1",
    "ipfm0024_3",
    "ipfm63_1",
    "ipfm63_3",
    "ipfm99_1",
    "ipfm99_3",
    "w6_basecli_inv",
    "cliact14",
    "cliact3",
    "isic_lg",
    "do_dest",
]
```

**Grand Total: 29 distinct file groups**

---

## File Groups with Date Columns (Priority Audit)

Based on schemas, these file groups likely have date columns:

**High Priority** (many date columns):
1. `polic_cu_azec` - ✅ **FIXED**
2. `ipf` - DTCREPOL, DTEFFAN, DTTRAAN, DTRESILP, DTECHANN, etc. (14+ date cols)
3. `constrcu` - DATOUVCH, DATFINCH, DATRECEP
4. `capitxcu_azec` - Unknown
5. `incendcu_azec` - Unknown

**Medium Priority** (few date columns):
6. `table_pt_gest` - Unknown
7. `ref_mig_azec_vs_ims` - Unknown

**Low Priority** (likely no dates):
- `table_segmentation_azec_mml` (product codes only)
- `lob` (product codes only)
- `segmprdt_*` (product codes only)
- `cproduit`, `prdcap` (product codes only)

---

## Recommended Audit Script Update

Update `FILE_GROUPS_TO_AUDIT` in `scripts/date_format_audit.py` to:

```python
# Focus on file groups WITH date columns
FILE_GROUPS_TO_AUDIT = [
    # High priority (many dates)
    "polic_cu_azec",       # AZEC - ✅ FIXED
    "ipf",              # AZ
    "constrcu",            # AZEC
    "capitxcu_azec",       # AZEC
    "incendcu_azec",       # AZEC
    
    # Medium priority
    "table_pt_gest",       # AZEC
    "ref_mig_azec_vs_ims", # AZEC
    "ipfm0024_1",          # CONSOLIDATION
    "ipfm0024_3",          # CONSOLIDATION
    "ipfm63_1",            # CONSOLIDATION
    "ipfm63_3",            # CONSOLIDATION
    "ipfm99_1",            # CONSOLIDATION
    "ipfm99_3",            # CONSOLIDATION
    "do_dest",             # CONSOLIDATION
    
    # Low priority (verify if needed)
    "rcentcu_azec",
    "risteccu_azec",
    "mulprocu_azec",
    "mpacu_azec",
]
```
