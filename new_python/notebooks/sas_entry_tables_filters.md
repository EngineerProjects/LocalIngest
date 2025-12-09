# SAS Project - Entry Tables Reading Filters and Conditions

This document details all filters, WHERE clauses, renames, and selection conditions applied when reading each entry table from external sources.

---

## 1. PTF_MVTS_AZEC_MACRO.sas

### 1.1. POLIC_CU.POLIC_CU (AZEC Policy Data)

```python
# Reading conditions
WHERE = {
    'intermed': 'NOT IN ("24050", "40490")',  # Exclude fictitious brokers
    'POLICE': 'NOT IN ("012684940")',  # Exclude fictitious policy
    'duree': 'NOT IN ("00") OR produit IN ("DO0", "TRC", "CTR", "CNR")',  # Exclude temporary except specific products
    'datfin': '!= effetpol',  # Exclude contracts without effect
    'Gestsit': 'NOT IN ("MIGRAZ") OR (GESTSIT = "MIGRAZ" AND ETATPOL = "R")'  # Exclude migrated contracts except specific
}

# Calculated fields at read
CALCULATED_AT_READ = {
    'DTECHANN': 'mdy(echeanmm, echeanjj, &annee.)',  # Termination date
    'PARTCIE': 'partbrut / 100',  # Company share percentage
    'TOP_LTA': 'CASE WHEN DUREE not in ("00", "01", "", " ") THEN 1 ELSE 0 END',  # Long-term agreement flag
    'TOP_REVISABLE': 'CASE WHEN INDREGUL="O" THEN 1 ELSE 0 END'  # Revisable contract flag
}

# Data updates after read
UPDATES = {
    'datexpir': 'SET datexpir = datfin WHERE (datfin > datexpir and ETATPOL in ("X","R"))',
    'NBAFN_ANTICIPE': 'SET = 1 WHERE (effetpol) > (&finmois.)',
    'NBRES_ANTICIPE': 'SET = 1 WHERE (datfin) > (&finmois.)',
    # Tacit renewal contracts not invoiced for > 1 year
    'ETATPOL_tacite': 'SET ETATPOL="R", DATFIN=DATTERME, DATRESIL=DATTERME WHERE DUREE="01" AND FINPOL=. AND DATTERME<>. AND DATTERME < Mdy(&mois., 01, &annee. - 1)',
    # Temporary contracts
    'ETATPOL_temp': 'SET ETATPOL="R", DATFIN=FINPOL, DATRESIL=FINPOL WHERE FINPOL<>. And DATFIN=.'
}

# Migration filter (vision-dependent)
MIGRATION_FILTER = {
    'vision': '> 202009',
    'join': 'LEFT JOIN Mig_azec.ref_mig_azec_vs_ims ON (POLICE = NOPOL_AZEC)',
    'filter': 'NBPTF_NON_MIGRES_AZEC = CASE WHEN NOPOL_AZEC IS missing THEN 1 ELSE 0 END'
}
```

### 1.2. CAPITXCU.CAPITXCU (AZEC Capital Data)

```python
# No WHERE filter at read
WHERE = None

# Transformations in DATA step
TRANSFORMATIONS = {
    'LCI_PE_100': 'IF smp_sre = "LCI" and brch_rea = "IP0" THEN capx_100',
    'LCI_PE_CIE': 'IF smp_sre = "LCI" and brch_rea = "IP0" THEN capx_cua',
    'LCI_DD_100': 'IF smp_sre = "LCI" and brch_rea = "ID0" THEN capx_100',
    'LCI_DD_CIE': 'IF smp_sre = "LCI" and brch_rea = "ID0" THEN capx_cua',
    'LCI_100': 'LCI_PE_100 + LCI_DD_100',  # Global LCI
    'LCI_CIE': 'LCI_PE_CIE + LCI_DD_CIE',
    'SMP_PE_100': 'IF smp_sre = "SMP" and brch_rea = "IP0" THEN capx_100',
    'SMP_PE_CIE': 'IF smp_sre = "SMP" and brch_rea = "IP0" THEN capx_cua',
    'SMP_DD_100': 'IF smp_sre = "SMP" and brch_rea = "ID0" THEN capx_100',
    'SMP_DD_CIE': 'IF smp_sre = "SMP" and brch_rea = "ID0" THEN capx_cua',
    'SMP_100': 'SMP_PE_100 + SMP_DD_100',  # Global SMP
    'SMP_CIE': 'SMP_PE_CIE + SMP_DD_CIE'
}

# Aggregation
GROUP_BY = ['POLICE', 'PRODUIT']
AGGREGATIONS = {
    'SMP_100': 'SUM(SMP_100)',
    'SMP_CIE': 'SUM(SMP_CIE)',
    'LCI_100': 'SUM(LCI_100)',
    'LCI_CIE': 'SUM(LCI_CIE)'
}

# Rename at output
RENAMES = {
    'POLICE': 'NOPOL',
    'PRODUIT': 'CDPROD'
}
```

### 1.3. INCENDCU.INCENDCU (Fire Insurance Data)

```python
# For NAF code extraction
SELECT_NAF = {
    'columns': ['POLICE', 'COD_NAF'],
    'distinct': True
}

# For TRE code extraction
SELECT_TRE = {
    'columns': ['POLICE', 'COD_TRE'],
    'distinct': True,
    'group_by': 'POLICE',
    'having': 'COD_TRE = min(COD_TRE)'  # Keep minimum TRE code per policy
}

# For capital amounts (PE/RD)
SELECT_CAPITAL = {
    'columns': ['POLICE', 'PRODUIT', 'MT_BASPE', 'MT_BASDI'],
    'group_by': ['POLICE', 'PRODUIT'],
    'aggregations': {
        'PERTE_EXP': 'sum(MT_BASPE)',
        'RISQUE_DIRECT': 'sum(MT_BASDI)',
        'VALUE_INSURED': 'sum(MT_BASDI + MT_BASPE)'
    }
}

# Rename at output
RENAMES_CAPITAL = {
    'PERTE_EXP': 'PERTE_EXP_100_IND',
    'RISQUE_DIRECT': 'RISQUE_DIRECT_100_IND',
    'VALUE_INSURED': 'VALUE_INSURED_100_IND'
}
```

### 1.4. RCENTCU.RCENTCU (RC Enterprise)

```python
# For NAF code
SELECT_NAF = {
    'columns': ['POLICE', 'COD_NAF'],
    'distinct': True
}

# For formulas (niches)
SELECT_FORMULE = {
    'columns': ['POLICE', 'FORMULE', 'FORMULE2', 'FORMULE3', 'FORMULE4'],
    'distinct': True
}

# Final filter: keep only unique policies
FINAL_FILTER = {
    'group_by': 'POLICE',
    'having': 'count(police) = 1'  # Keep only policies with single formula
}
```

### 1.5. RISTECCU.RISTECCU (Professional Risk)

```python
# Same as RCENTCU
SELECT_NAF = {
    'columns': ['POLICE', 'COD_NAF'],
    'distinct': True
}

SELECT_FORMULE = {
    'columns': ['POLICE', 'FORMULE', 'FORMULE2', 'FORMULE3', 'FORMULE4'],
    'distinct': True
}

FINAL_FILTER = {
    'group_by': 'POLICE',
    'having': 'count(police) = 1'
}
```

### 1.6. MULPROCU.MULPROCU (Multi-risk)

```python
# Aggregation for turnover
SELECT = {
    'columns': ['POLICE', 'CHIFFAFF'],
    'group_by': 'POLICE',
    'aggregations': {
        'MTCA': 'SUM(CHIFFAFF)'
    }
}
```

### 1.7. MPACU.MPACU (MPA)

```python
# For NAF code only
SELECT = {
    'columns': ['POLICE', 'COD_NAF'],
    'distinct': True
}
```

### 1.8. CONSTRCU.CONSTRCU (Construction Sites - AZEC)

```python
# No filter at read
WHERE = None

# Selected columns
KEEP_COLUMNS = [
    'POLICE', 'PRODUIT', 'DATOUVCH', 'LDESTLOC', 'MNT_GLOB',
    'DATRECEP', 'DEST_LOC', 'DATFINCH', 'LQUALITE'
]
```

### 1.9. NAF_2008.IRD_SUIVI_ENGAGEMENTS_&vision

```python
# Version check
VERSION_CHECK = {
    'condition': '&vision. >= 202103',
    'action_if_false': 'Create empty columns CDNAF2008_TEMP=" ", ISIC_CODE_SUI_TEMP=" "'
}

# File decompression before read
PRE_READ = {
    'command': 'gunzip &vue./ird_suivi_engagements_&vision..sas7bdat.gz'
}

# Reading options
READ_OPTIONS = {
    'encoding': 'any'
}

# Post-read compression
POST_READ = {
    'command': 'gzip &vue./*.sas7bdat'
}

# Join conditions
JOIN = {
    'type': 'LEFT JOIN',
    'on': ['t1.NOPOL = t2.NOPOL', 't1.CDPROD = t2.CDPROD']
}

# Selected columns
SELECT = ['CDNAF08', 'CDISIC']
RENAME = {
    'CDNAF08': 'CDNAF2008_TEMP',
    'CDISIC': 'ISIC_CODE_SUI_TEMP'
}
```

### 1.10. PT_GEST.PTGST_*

```python
# Version selection logic
VERSION_SELECTION = {
    'function': 'derniere_version(PT_GEST, ptgst_, &vision., TABLE_PT_GEST)',
    'description': 'Automatically selects the most recent PTGST table <= vision'
}

# Selected columns
SELECT = ['PTGST', 'UPPER_MID']
```

---

## 2. CODIFICATION_ISIC_CONSTRUCTION.sas

### 2.1. REF_ISIC.MAPPING_CDNAF2003_ISIC_*

```python
# Version selection
VERSION_SELECTION = {
    'function': 'derniere_version(REF_ISIC, MAPPING_CDNAF2003_ISIC_, &vision., TABLE_REF_ISIC_03)'
}

# Join conditions (with source table)
JOIN = {
    'type': 'LEFT JOIN',
    'on': 't2.CDNAF_2003 = t1.CDNAF AND t1.CMARCH="6"'  # Only construction market
}

# Selected columns
SELECT = ['CDNAF_2003', 'ISIC_CODE']
```

### 2.2. REF_ISIC.MAPPING_CDNAF2008_ISIC_*

```python
# Version selection
VERSION_SELECTION = {
    'function': 'derniere_version(REF_ISIC, MAPPING_CDNAF2008_ISIC_, &vision., TABLE_REF_ISIC_08)'
}

# Join conditions
JOIN = {
    'type': 'LEFT JOIN',
    'on': 't3.CDNAF_2008 = t1.CDNAF2008_TEMP AND t1.CMARCH="6"'
}

# Selected columns
SELECT = ['CDNAF_2008', 'ISIC_CODE']
```

### 2.3. ISIC_CST.MAPPING_ISIC_CONST_ACT_*

```python
# Version selection
VERSION_SELECTION = {
    'function': 'derniere_version(ISIC_CST, MAPPING_ISIC_CONST_ACT_, &vision., TABLE_ISIC_CST_ACT)'
}

# Join conditions (for activity contracts)
JOIN = {
    'type': 'LEFT JOIN',
    'on': [
        't1.ACTPRIN = t2.ACTPRIN',
        't1.CMARCH="6"',
        't1.CDNATP="R"'  # Only renewable contracts
    ]
}

# Selected columns
SELECT = ['ACTPRIN', 'CDNAF08', 'CDTRE', 'CDNAF03', 'CDISIC']
RENAME = {
    'CDNAF08': 'CDNAF08_CONST_R',
    'CDTRE': 'CDTRE_CONST_R',
    'CDNAF03': 'CDNAF03_CONST_R',
    'CDISIC': 'CDISIC_CONST_R'
}
```

### 2.4. ISIC_CST.MAPPING_ISIC_CONST_CHT_*

```python
# Version selection
VERSION_SELECTION = {
    'function': 'derniere_version(ISIC_CST, MAPPING_ISIC_CONST_CHT_, &vision., TABLE_ISIC_CST_CHT)'
}

# Join conditions (for construction sites)
JOIN = {
    'type': 'LEFT JOIN',
    'on': [
        't1.DESTI_ISIC = t2.DESTI_ISIC',
        't1.CMARCH="6"',
        't1.CDNATP="C"'  # Only construction sites
    ]
}

# DESTI_ISIC calculation (keyword analysis)
DESTI_ISIC_RULES = {
    'RESIDENTIEL': [
        'PRXMATCH("/(COLL)/", UPCASE(DSTCSC))',
        'PRXMATCH("/(LOGE)/", UPCASE(DSTCSC))',
        'PRXMATCH("/(APPA)/", UPCASE(DSTCSC))',
        'PRXMATCH("/(HABIT)/", UPCASE(DSTCSC))',
        'PRXMATCH("/(EHPAD)/", UPCASE(DSTCSC))',
        'PRXMATCH("/(RESID)/", UPCASE(DSTCSC))',
        'PRXMATCH("/(BAT)/", UPCASE(DSTCSC))',
        'PRXMATCH("/(LGTS)/", UPCASE(DSTCSC))',
        'PRXMATCH("/(HAB)/", UPCASE(DSTCSC))',
        'DSTCSC in ("01", "02", "03", "03+22", "04", "06", "08", "1", "2", "3", "4", "6", "8")'
    ],
    'MAISON': [
        'PRXMATCH("/(MAISON)/", UPCASE(DSTCSC))',
        'PRXMATCH("/(INDIV)/", UPCASE(DSTCSC))',
        'DSTCSC in ("22")'
    ],
    'INDUSTRIE': [
        'PRXMATCH("/(INDUS)/", UPCASE(DSTCSC))',
        'PRXMATCH("/(GARAGE)/", UPCASE(DSTCSC))',
        'PRXMATCH("/(TELECOM)/", UPCASE(DSTCSC))',
        'DSTCSC in ("12", "18", "19")'
    ],
    'BUREAU': [
        'PRXMATCH("/(BUREAU)/", UPCASE(DSTCSC))',
        'PRXMATCH("/(HOTEL)/", UPCASE(DSTCSC))',
        'PRXMATCH("/(TOURIS)/", UPCASE(DSTCSC))',
        'PRXMATCH("/(LOIS)/", UPCASE(DSTCSC))',
        'PRXMATCH("/(ECOL)/", UPCASE(DSTCSC))',
        'PRXMATCH("/(MED)/", UPCASE(DSTCSC))',
        'PRXMATCH("/(BANC)/", UPCASE(DSTCSC))',
        'PRXMATCH("/(RESTAURA)/", UPCASE(DSTCSC))',
        'DSTCSC in ("05", "07", "09", "10", "13", "14", "16", "5", "7", "9")'
    ],
    'COMMERCE': [
        'PRXMATCH("/(COMMER)/", UPCASE(DSTCSC))',
        'PRXMATCH("/(CIAL)/", UPCASE(DSTCSC))',
        'PRXMATCH("/(SPOR)/", UPCASE(DSTCSC))',
        'PRXMATCH("/(AQUAT)/", UPCASE(DSTCSC))',
        'DSTCSC in ("17", "23")'
    ],
    'HOPITAL': [
        'PRXMATCH("/(CLINI )/", UPCASE(DSTCSC))',
        'PRXMATCH("/(HOP)/", UPCASE(DSTCSC))',
        'PRXMATCH("/(HOSP)/", UPCASE(DSTCSC))',
        'PRXMATCH("/(CHIR)/", UPCASE(DSTCSC))',
        'DSTCSC in ("15")'
    ],
    'INDUSTRIE_LIGHT': [
        'PRXMATCH("/(STOCK)/", UPCASE(DSTCSC))',
        'PRXMATCH("/(SUPPORT)/", UPCASE(DSTCSC))',
        'PRXMATCH("/(AGRIC)/", UPCASE(DSTCSC))',
        'PRXMATCH("/(ENTREPOT)/", UPCASE(DSTCSC))',
        'PRXMATCH("/(HANGAR)/", UPCASE(DSTCSC))',
        'DSTCSC in ("11", "20", "21")'
    ],
    'VOIRIE': [
        'PRXMATCH("/(VRD)/", UPCASE(DSTCSC))',
        'PRXMATCH("/(PARK)/", UPCASE(DSTCSC))',
        'PRXMATCH("/(STATIONNEMENT)/", UPCASE(DSTCSC))',
        'DSTCSC in ("24", "26", "28")'
    ],
    'VENTE': [
        'CDPROD = "01059"'  # Specific product
    ],
    'PHOTOV': [
        'PRXMATCH("/(PHOTOV)/", UPCASE(DSTCSC))'
    ],
    'AUTRES_GC': [
        'PRXMATCH("/(NON SOUMIS)/", UPCASE(DSTCSC))',
        'DSTCSC in ("27", "99")'
    ],
    'AUTRES_BAT': [
        'PRXMATCH("/(SOUMIS)/", UPCASE(DSTCSC))',
        'DEFAULT if ISIC_CODE_TEMP = "" and DESTI_ISIC = ""'
    ]
}

# Selected columns
SELECT = ['DESTI_ISIC', 'CDNAF08', 'CDTRE', 'CDNAF03', 'CDISIC']
RENAME = {
    'CDNAF08': 'CDNAF08_CONST_C',
    'CDTRE': 'CDTRE_CONST_C',
    'CDNAF03': 'CDNAF03_CONST_C',
    'CDISIC': 'CDISIC_CONST_C'
}
```

### 2.5. REF_ISIC.TABLE_ISIC_TRE_NAF_*

```python
# Version selection
VERSION_SELECTION = {
    'condition': '&vision. >= 202305',
    'if_true': 'derniere_version(REF_ISIC, table_isic_tre_naf_, &vision., TABLE_REF_ISIC)',
    'if_false': 'USE table_isic_tre_naf_202305'
}

# Join conditions
JOIN = {
    'type': 'LEFT JOIN',
    'on': 't2.ISIC_CODE = t1.ISIC_CODE_TEMP'
}

# Selected columns
SELECT = [
    'ISIC_CODE',
    'HAZARD_GRADES_FIRE',
    'HAZARD_GRADES_BI',
    'HAZARD_GRADES_RCA',
    'HAZARD_GRADES_RCE',
    'HAZARD_GRADES_TRC',
    'HAZARD_GRADES_RCD',
    'HAZARD_GRADES_DO'
]

# All columns renamed with _TEMP suffix during processing
```

---

## 3. EMISSIONS_RUN.sas

### 3.1. PRM.RF_FR1_PRM_DTL_MIDCORP_M (One BI Premium Data)

```python
# Remote connection
REMOTE = {
    'server': 'biaa-sg-prod.srv.allianz',
    'port': 7556,
    'encoding': 'utf-8'
}

# Main filter
WHERE = {
    'cd_marche': 'IN ("6")',  # Construction market only
    'DT_CPTA_CTS': '<= "&vision"'  # Accounting date <= vision
}

# Excluded intermediaries
EXCLUDED_INTERMEDIARIES = [
    '102030', '446000', '446118', '446218', '482001', '489090',
    '500150', '4A1400', '4A1500', '4A1600', '4A1700', '4A1800',
    '4A1900', '4F1004', '5B2000', '5R0001', 'H90036', 'H90037',
    'H90059', 'H90061', 'H99045', 'H99059'
]

# Additional filters after initial read
ADDITIONAL_FILTERS = {
    'CD_INT_STC': f'NOT IN {EXCLUDED_INTERMEDIARIES}',
    'product_guarantee': 'NOT (substr(CD_PRD_PRM, 1, 2) IN ("19") AND CD_GAR_PROSPCTIV IN ("220"))',
    'intermediary_product': 'NOT (CD_INT_STC in ("567ME0") AND CD_PRD_PRM in ("00200"))',
    'guarantee': 'NOT (CD_GAR_PROSPCTIV IN ("180","183","184","185"))',
    'product': 'NOT (CD_PRD_PRM in ("01073"))',
    'category': 'NOT (CD_CAT_MIN in ("792","793") OR CD_GAR_PROSPCTIV="00400")'
}

# Calculated fields
CALCULATED = {
    'CGAR': 'compress(substr(cd_gar_prospctiv, 3, 3))',
    'EXERCICE': 'CASE WHEN compress(NU_EX_RATT_CTS) >= "&annee." THEN "cou" ELSE "ant" END',
    'CDPOLE': '''CASE
        WHEN CD_NIV_2_STC in("DCAG","DCPS","DIGITAL") THEN "1"
        WHEN CD_NIV_2_STC = "BROKDIV" THEN "3"
    END''',
    'DIRCOM': '"AZ "',
    'MTCOM': 'MT_CMS_CTS'
}

# Aggregations
AGGREGATIONS = {
    'group_by': ['CDPOLE', 'NOINT', 'NOPOL', 'CDPROD', 'CD_GAR_PRINC', 'CD_GAR_PROSPCTIV', 'DIRCOM', 'CD_CAT_MIN'],
    'metrics': {
        'PRIMES_X': 'SUM(MT_HT_CTS)',
        'PRIMES_N': 'SUM(MT_HT_CTS) WHERE EXERCICE="cou"',
        'MTCOM_X': 'SUM(MTCOM)'
    }
}

# Renames at output
RENAMES = {
    'NU_CNT_PRM': 'NOPOL',
    'CD_PRD_PRM': 'CDPROD',
    'CD_INT_STC': 'NOINT'
}
```

### 3.2. SEG.SEGMENTPRDT_&vision

```python
# Join conditions
JOIN = {
    'type': 'LEFT JOIN',
    'on': ['t1.CDPROD = t2.CPROD', 't1.CDPOLE = t2.CDPOLE']
}

# Filter after join
WHERE = {
    'CMARCH': '= "6"'  # Construction market only
}

# Selected columns
SELECT = ['CPROD', 'CDPOLE', 'CMARCH', 'CSEG', 'CSSSEG']
```

---

## 4. PTF_MVTS_AZ_MACRO.sas

### 4.1. PTF16.IPF / PTF36.IPF (Portfolio Agent/Courtage)

```python
# Remote connection
REMOTE = {
    'server': 'STP3',
    'port': 7013,
    'encoding': 'native'
}

# Library selection (current vs historical)
LIBRARY_SELECTION = {
    'current': {
        'condition': '&annee = &ASYS. and &mois. = &MSYS.',
        'PTF16': 'INFP.IIA0P6$$.IPFE16',
        'PTF36': 'INFP.IIA0P6$$.IPFE36'
    },
    'historical': {
        'condition': 'ELSE',
        'PTF16': 'INFH.IIMMP6$$.IPFE16.G0&AMN0.V00',
        'PTF36': 'INFH.IIMMP6$$.IPFE36.G0&AMN0.V00'
    }
}

# Main filters
WHERE = {
    'cdri': 'NOT IN ("X")',  # Exclude specific status
    'cdsitp': 'NOT IN ("4","5")',  # Exclude specific situation types
    'noint': '''NOT IN (
        "H90061","482001","489090","102030","H90036","H90059","H99045","H99059",
        "5B2000","446000","5R0001","446118","4F1004","4A1400","4A1500","4A1600",
        "4A1700","4A1800","4A1900","482001","489090","4F1004","4L1010"
    )''',
    'cdprod': 'NOT IN ("01073")',
    'cdnatp': 'IN ("R","O","T","C")',  # Renewable, Occasional, Temporary, Construction site
    'cmarch': '= "6"',  # Construction market
    'csegt': '= "2"'  # Segment 2
}

# Calculated fields at read
CALCULATED = {
    'CDPOLE': '"1" for PTF16, "3" for PTF36',
    'VISION': '&vision.',
    'EXEVUE': '&annee.',
    'MOISVUE': '&mois.',
    'MTCA': 'FNCMACA',
    'TX': 'CASE WHEN txcede = . THEN 0 ELSE txcede END',
    'TOP_COASS': 'CASE WHEN cdpolqpl = "1" THEN 1 ELSE 0 END',
    'COASS': '''CASE
        WHEN cdpolqpl = "1" AND CDTPCOA in ("3", "6") THEN "APERITION"
        WHEN cdpolqpl = "1" AND CDTPCOA in ("4", "5") THEN "COASS. ACCEPTEE"
        WHEN cdpolqpl = "1" AND CDTPCOA in ("8") THEN "ACCEPTATION INTERNATIONALE"
        WHEN cdpolqpl = "1" AND CDTPCOA not in ("3" "4" "5" "6" "8") THEN "AUTRES"
        WHEN cdpolqpl <> "1" THEN "SANS COASSURANCE"
    END''',
    'PARTCIE': '''CASE
        WHEN cdpolqpl <> "1" THEN 1
        WHEN cdpolqpl = "1" THEN (PRCDCIE / 100)
    END''',
    'TOP_REVISABLE': 'CASE WHEN CDPOLRVI= "1" THEN 1 ELSE 0 END',
    'CRITERE_REVISION': '''CASE
        WHEN CDGREV ="20" THEN "20_SANS_PROV_MANUEL "
        WHEN CDGREV ="21" THEN "21_PROV_FIXE_MANUEL "
        WHEN CDGREV ="22" THEN "22_PROV_VAR_MANUEL "
        WHEN CDGREV ="23" THEN "23_SANS_REVISION "
        WHEN CDGREV ="24" THEN "24_REV_FIN_CHANTIER "
        WHEN CDGREV ="30" THEN "30_SANS_PROV_MANUEL "
        WHEN CDGREV ="31" THEN "31_PROV_FIXE_MANUEL "
        WHEN CDGREV ="32" THEN "32_PROV_VAR_MANUEL "
        WHEN CDGREV ="40" THEN "40_SANS_PROV_AUTOMAT"
        WHEN CDGREV ="41" THEN "41_PROV_FIXE_AUTOMAT"
        WHEN CDGREV ="42" THEN "42_PROV_VAR_AUTOMAT "
        WHEN CDGREV ="43" THEN "43_SANS_REV_AUTOMAT "
        WHEN CDGREV ="80" THEN "80_REVISION_SPECIALE"
        WHEN CDGREV ="90" THEN "90_SANS_PROV_AUTOMAT"
        WHEN CDGREV ="91" THEN "91_PROV_FIXE_AUTOMAT"
        WHEN CDGREV ="92" THEN "92_PROV_VAR_AUTOMAT "
    END''',
    'DIRCOM': '"AZ "'
}

# Renames
RENAMES = {
    'posacta': 'posacta_ri',
    'rueacta': 'rueacta_ri',
    'cediacta': 'cediacta_ri',
    'csegt': 'CSEG',
    'cssegt': 'CSSSEG',
    'CDTPCOA': 'CDCOAS',
    'PRCDCIE': 'PRCIE'
}

# Capital field processing (14 fields: MTCAPI1-14, LBCAPI1-14)
CAPITAL_PROCESSING = {
    'loop': 'k=1 to 14',
    'LCI_100': {
        'condition': '''index(lbcapi&k.,"LCI GLOBAL DU CONTRAT") > 0
            or index(lbcapi&k.,"CAPITAL REFERENCE OU LCI") > 0
            or index(lbcapi&k.,"LCI IRD (DOM. DIRECTS+P.EXPLOIT)") > 0
            or index(lbcapi&k.,"LIMITE CONTRACTUELLE INDEMNITE") > 0''',
        'value': 'mtcapi&k.'
    },
    'SMP_100': {
        'condition': '''index(lbcapi&k.,"SMP GLOBAL DU CONTRAT") > 0
            or index(lbcapi&k.,"SMP RETENU") > 0
            or index(lbcapi&k.,"SINISTRE MAXIMUM POSSIBLE") > 0''',
        'value': 'mtcapi&k.'
    },
    'RISQUE_DIRECT': {
        'condition': '''(index(lbcapi&k.,"RISQUE DIRECT") > 0
            and lbcapi&k. not in ("SINIS MAX POSSIBLE RISQUE DIRECT"))
            or index(lbcapi&k.,"CAPITAUX DOMMAGES DIR") > 0''',
        'value': 'mtcapi&k.'
    },
    'PERTE_EXP': {
        'condition': '''index(lbcapi&k., "PERTE EXPLOITATION (MARGE BRUTE)") > 0
            or index(lbcapi&k., "PERTE D EXPLOITATION") > 0
            or index(lbcapi&k., "PERTE D\'EXPLOITATION") > 0
            or index(lbcapi&k., "CAPITAL PERTES EXPLOITATION") > 0
            or index(lbcapi&k., "CAPITAUX TOTAUX P.E.") > 0
            or index(lbcapi&k., "PERTE EXPLOITATION/AUT. DOM. MAT") > 0
            or index(lbcapi&k., "PERTES D\'EXPLOITATION") > 0
            or index(lbcapi&k., "PERTES EXPLOITATION") > 0''',
        'value': 'mtcapi&k.'
    }
}

# Business indicators calculation
INDICATORS = {
    'primeto': 'mtprprto * (1 - tx / 100)',
    'TOP_LTA': 'SET = 1 WHERE ((CTDUREE > 1) and (tydris1 in ("QAW" "QBJ" "QBK" "QBB" "QBM")))',

    # Portfolio
    'NBPTF': '''SET = 1 WHERE (CSSSEG ne "5"
        and CDNATP in ("R","O")
        and ((cdsitp in("1") and dtcrepol <= "&DTFIN."d)
             or (cdsitp = "3" and dtresilp > "&DTFIN."d)))''',
    'Primes_PTF': '= primeto WHERE NBPTF = 1',

    # New business (AFN)
    'NBAFN': '''SET = 1 WHERE (
        (("&DTDEB_AN"d <= dteffan <= "&DTFIN"d) and ("&DTDEB_AN"d <= dttraan <= "&DTFIN"d))
        or (("&DTDEB_AN"d <= dtcrepol <= "&DTFIN"d))
        or (year(dteffan) < year("&DTFIN"d) and ("&DTDEB_AN"d <= dttraan <= "&DTFIN"d))
    )''',
    'Primes_AFN': '= primeto WHERE NBAFN = 1',

    # Terminations (RES)
    'NBRES': '''SET = 1 WHERE CDNATP ne "C"
        and ((cdsitp="3" and ("&DTDEB_AN"d <= dtresilp <= "&DTFIN"d))
             or (cdsitp="3" and ("&DTDEB_AN"d <= dttraar <= "&DTFIN"d) and dtresilp <= "&DTFIN"d))''',
    'Primes_RES': '= primeto WHERE NBRES = 1',

    # Replaced contracts (RPC)
    'NBRPC': '''SET = 1 WHERE NBRES = 1
        And ((cdtypli1 in("RP") and year(dttypli1)=year("&DTFIN"d))
             or (cdtypli2 in("RP") and year(dttypli2)=year("&DTFIN"d))
             or (cdtypli3 in("RP") and year(dttypli3)=year("&DTFIN"d)))''',
    'adjust_NBRES_RPC': 'SET NBRES = 0, Primes_RES = 0 WHERE NBRPC = 1',

    # Replacement contracts (RPT)
    'NBRPT': '''SET = 1 WHERE NBAFN = 1
        And ((cdtypli1 in("RE") and year(dttypli1)=year("&DTFIN"d))
             or (cdtypli2 in("RE") and year(dttypli2)=year("&DTFIN"d))
             or (cdtypli3 in("RE") and year(dttypli3)=year("&DTFIN"d)))''',
    'adjust_NBAFN_RPT': 'SET NBAFN = 0, Primes_AFN = 0 WHERE NBRPT = 1',

    # Temporary contracts
    'TOP_TEMP': 'SET = 1 WHERE cdnatp = "T"'
}

# Exposure calculation
EXPOSURE = {
    'expo_ytd': '''(MAX(0,(MIN(dtresilp, "&DTFIN"d) - MAX(dtcrepol, "&DTDEB_AN"d)) + 1))
        / ((mdy(12,31,&annee.) - "&DTDEB_AN"d) + 1)''',
    'expo_gli': '''(MAX(0,(MIN(dtresilp, "&DTFIN"d) - MAX(dtcrepol, "&dtfinmn1"d+1)) + 1))
        / ("&DTFIN"d - "&dtfinmn1"d)''',
    'DT_DEB_EXPO': 'MAX(dtcrepol, "&DTDEB_AN"d)',
    'DT_FIN_EXPO': 'MIN(dtresilp, "&DTFIN"d)',
    'condition': '''WHERE (cdnatp in ("R","O")
        and ((cdsitp = "1" and dtcrepol <= "&dtfinmn"d)
             or (cdsitp="3" and dtresilp > "&dtfinmn"d)))
        or (cdsitp in ("1" "3")
            and ((dtcrepol <= "&dtfinmn"d and (dtresilp=. or "&dtfinmn1"d <= dtresilp < "&dtfinmn"d))
                 or (dtcrepol <= "&dtfinmn"d and dtresilp > "&dtfinmn"d)
                 or ("&dtfinmn1"d < dtcrepol <= "&dtfinmn"d and "&dtfinmn1"d <= dtresilp))
            and cdnatp not in ("F"))'''
}

# Premium and turnover calculations
PREMIUM_CA = {
    'PRCDCIE_default': 'SET PRCDCIE = 100 WHERE PRCDCIE = . or PRCDCIE = 0',
    'Cotis_100_base': 'SET Cotis_100 = mtprprto',
    'Cotis_100_coass': 'SET Cotis_100 = (mtprprto * 100) / prcdcie WHERE (TOP_COASS = 1 AND CDCOAS in ("4", "5"))',
    'MTCA_init': 'SET MTCA = 0 WHERE MTCA = .',
    'MTCAF_init': 'SET MTCAF = 0 WHERE MTCAF = .',
    'MTCA_': 'MTCAF + MTCA'
}

# Business rule adjustments
ADJUSTMENTS = {
    'TOP_AOP': 'SET = 1 WHERE OPAPOFFR = "O"',
    'NBAFN_ANTICIPE': 'SET = 1 WHERE ((dteffan > &finmois.) OR (dtcrepol > &finmois.)) AND (NOT (cdtypli1 in("RE") or cdtypli2 in("RE") or cdtypli3 in("RE")))',
    'NBRES_ANTICIPE': 'SET = 1 WHERE (dtresilp > &FINMOIS.) AND (NOT (cdtypli1 in("RP") or cdtypli2 in("RP") or cdtypli3 in("RP") or cdmotres="R" or cdcASres="2R"))'
}

# Data cleanup
CLEANUP = {
    'EXPO_YTD_zero': 'SET DT_DEB_EXPO = ., DT_FIN_EXPO = . WHERE EXPO_YTD = 0',
    'NMCLT_empty': 'If NMCLT in(" ") then NMCLT=NMACTA'
}
```

### 4.2. PTF16a.IPFM99 / PTF36a.IPFM99 (Product 01099)

```python
# Filter
WHERE = {
    'CDPROD': '= "01099"'
}

# Selected columns
SELECT = ['CDPOLE', 'VISION', 'CDPROD', 'NOPOL', 'NOINT', 'MTCA', 'MTCAENP', 'MTCASST', 'MTCAVNT']

# Join with main portfolio
JOIN = {
    'type': 'LEFT JOIN',
    'on': ['a.CDPOLE = b.CDPOLE', 'a.CDPROD = b.CDPROD', 'a.NOPOL = b.NOPOL', 'a.NOINT = b.NOINT']
}

# Update turnover for product 01099
UPDATE = {
    'MTCA': 'SET = MTCAENP + MTCASST + MTCAVNT WHERE CDPROD = "01099"'
}
```

### 4.3. SEGMprdt.PRDPFA1 / PRDPFA3 (Segmentation)

```python
# Filter
WHERE = {
    'cmarch': 'IN ("6")'  # Construction market only
}

# Calculated fields
CALCULATED = {
    'lmarch2': 'cmarch!!"_"!!lmarch',
    'lseg2': 'cseg!!"_"!!lseg',
    'lssseg2': 'cssseg!!"_"!!lssseg',
    'reseau': '"1" for PRDPFA1, "3" for PRDPFA3'
}

# Keep columns
KEEP = ['CPROD', 'lprod', 'cseg', 'lseg2', 'cssseg', 'lssseg2', 'cmarch', 'lmarch2', 'reseau']

# Deduplication
DEDUPLICATE_BY = ['CPROD']
```

### 4.4. PRDCAP.PRDCAP (Product Catalog)

```python
# Renames
RENAMES = {
    'cdprod': 'CPROD',
    'lbtprod': 'lprod'
}

# Keep columns
KEEP = ['CPROD', 'lprod']

# Deduplication
DEDUPLICATE_BY = ['CPROD']
```

---

## 5. CAPITAUX_AZ_MACRO.sas

### 5.1. PTF16.IPF / PTF36.IPF (Capital Focus)

```python
# Same remote connection as PTF_MVTS_AZ_MACRO

# Main filters (same as PTF movements but cdsitp different)
WHERE = {
    'cdri': 'NOT IN ("X")',
    'cdsitp': 'NOT IN ("5")',  # Note: excludes "5" but not "4" (different from movements)
    'noint': '''NOT IN (
        "H90061","482001","489090","102030","H90036","H90059","H99045","H99059",
        "5B2000","446000","5R0001","446118","4F1004","4A1400","4A1500","4A1600",
        "4A1700","4A1800","4A1900","482001","489090","4F1004","4L1010"
    )''',
    'cdprod': 'NOT IN ("01073")',
    'cdnatp': 'IN ("R","O","T")',  # Note: excludes "C" (construction sites)
    'cmarch': '= "6"',
    'csegt': '= "2"'
}

# Calculated fields
CALCULATED = {
    'CDPOLE': '"1" for PTF16, "3" for PTF36',
    'VISION': '&vision.',
    'EXEVUE': '&annee.',
    'MOISVUE': '&mois.',
    'TX': 'CASE WHEN txcede = . THEN 0 ELSE txcede END',
    'PARTCIE': '''CASE
        WHEN cdpolqpl <> "1" THEN 1
        WHEN cdpolqpl = "1" THEN (PRCDCIE / 100)
    END'''
}

# Renames
RENAMES = {
    'csegt': 'CSEG',
    'cssegt': 'CSSSEG',
    'CDTPCOA': 'CDCOAS'
}

# Capital indexation processing (14 fields with indexation)
INDEXATION = {
    'macro_call': '%indexation_v2(date=&FINMOIS., ind=&k.)',
    'loop': 'k=1 to 14',
    'indexed_fields': 'mtcapi&k.i (indexed value)',
    'parameters': {
        'DATE': '&FINMOIS.',
        'IND': '&k.',
        'NOMMT': 'MTCAPI',
        'NOMNAT': 'CDPRVB',
        'NOMIND': 'PRPRVC'
    }
}

# Capital calculation WITH indexation (_IND suffix)
CAPITAL_INDEXED = {
    'LCI_100_IND': {
        'condition': '''index(lbcapi&k.,"LCI GLOBAL DU CONTRAT") > 0
            or index(lbcapi&k.,"CAPITAL REFERENCE OU LCI") > 0
            or index(lbcapi&k.,"LCI IRD (DOM. DIRECTS+P.EXPLOIT)") > 0
            or index(lbcapi&k.,"LIMITE CONTRACTUELLE INDEMNITE") > 0''',
        'value': 'If mtcapi&k.i > LCI_100_IND Then LCI_100_IND = mtcapi&k.i'
    },
    'LIMITE_RC_100_PAR_SIN': {
        'condition': '''index(lbcapi&k.,"DOMMAGES CORPORELS") > 0
            or index(lbcapi&k.,"DOMM. MAT/IMMAT CONSEC EN COURS") > 0
            or index(lbcapi&k.,"TOUS DOMMAGES CONFONDUS") > 0''',
        'value': 'If mtcapi&k.i > LIMITE_RC_100_PAR_SIN Then LIMITE_RC_100_PAR_SIN = mtcapi&k.i'
    },
    'LIMITE_RC_100_PAR_AN': {
        'condition': '''index(lbcapi&k.,"TOUS DOMMAGES CONFONDUS (AL)") > 0
            or index(lbcapi&k.,"TOUS DOMMAGES CONFONDUS / ANN)") > 0
            or index(lbcapi&k.,"DOM MAT/IMMAT CONSEC (RC AL)") > 0
            or index(lbcapi&k.,"RCP TOUS DOM.CONFONDUS") > 0''',
        'value': 'If mtcapi&k.i > LIMITE_RC_100_PAR_AN Then LIMITE_RC_100_PAR_AN = mtcapi&k.i'
    },
    'SMP_100_IND': {
        'condition': '''index(lbcapi&k.,"SMP GLOBAL DU CONTRAT") > 0
            or index(lbcapi&k.,"SMP RETENU") > 0
            or index(lbcapi&k.,"SINISTRE MAXIMUM POSSIBLE") > 0''',
        'value': 'If mtcapi&k.i > SMP_100_IND Then SMP_100_IND = mtcapi&k.i'
    },
    'RISQUE_DIRECT_100_IND': {
        'condition': '''(index(lbcapi&k.,"RISQUE DIRECT") > 0 and lbcapi&k. not in ("SINIS MAX POSSIBLE RISQUE DIRECT"))
            or index(lbcapi&k.,"CAPITAUX DOMMAGES DIR") > 0''',
        'value': 'If mtcapi&k.i > RISQUE_DIRECT_100_IND Then RISQUE_DIRECT_100_IND = mtcapi&k.i'
    },
    'PERTE_EXP_100_IND': {
        'condition': '''index(lbcapi&k., "PERTE EXPLOITATION (MARGE BRUTE)") > 0
            or index(lbcapi&k., "PERTE D EXPLOITATION") > 0
            or index(lbcapi&k., "PERTE D\'EXPLOITATION") > 0
            or index(lbcapi&k., "CAPITAL PERTES EXPLOITATION") > 0
            or index(lbcapi&k., "CAPITAUX TOTAUX P.E.") > 0
            or index(lbcapi&k., "PERTE EXPLOITATION/AUT. DOM. MAT") > 0
            or index(lbcapi&k., "PERTES D\'EXPLOITATION") > 0
            or index(lbcapi&k., "PERTES EXPLOITATION") > 0''',
        'value': 'If mtcapi&k.i > PERTE_EXP_100_IND Then PERTE_EXP_100_IND = mtcapi&k.i'
    },
    'SMP_PE_100_IND': {
        'condition': 'index(lbcapi&k., "LCI/SMP PERTE EXPLOITATION") > 0',
        'value': 'SMP_PE_100_IND = mtcapi&k.i'
    },
    'SMP_RD_100_IND': {
        'condition': 'index(lbcapi&k., "SINIS MAX POSSIBLE RISQUE DIRECT") > 0',
        'value': 'SMP_RD_100_IND = mtcapi&k.i'
    }
}

# Capital calculation WITHOUT indexation (for comparison)
CAPITAL_NON_INDEXED = {
    'LCI_100': 'If mtcapi&k. > LCI_100 Then LCI_100 = mtcapi&k.',
    'SMP_100': 'If mtcapi&k. > SMP_100 Then SMP_100 = mtcapi&k.',
    'RISQUE_DIRECT_100': 'If mtcapi&k. > RISQUE_DIRECT_100 Then RISQUE_DIRECT_100 = mtcapi&k.',
    'PERTE_EXP_100': 'If mtcapi&k. > PERTE_EXP_100 Then PERTE_EXP_100 = mtcapi&k.',
    'SMP_PE_100': 'SMP_PE_100 = mtcapi&k.',
    'SMP_RD_100': 'SMP_RD_100 = mtcapi&k.'
}

# Normalization to 100%
NORMALIZATION = {
    'PRCDCIE_default': 'SET PRCDCIE = 100 WHERE PRCDCIE = . or PRCDCIE = 0',
    'all_capitals_to_100': '''SET
        PERTE_EXP_100_IND = (PERTE_EXP_100_IND * 100) / prcdcie,
        RISQUE_DIRECT_100_IND = (RISQUE_DIRECT_100_IND * 100) / prcdcie,
        VALUE_INSURED = (VALUE_INSURED * 100) / prcdcie,
        SMP_100_IND = (SMP_100_IND * 100) / prcdcie,
        LCI_100_IND = (LCI_100_IND * 100) / prcdcie,
        LIMITE_RC_100_PAR_SIN = (LIMITE_RC_100_PAR_SIN * 100) / prcdcie,
        LIMITE_RC_100_PAR_AN = (LIMITE_RC_100_PAR_AN * 100) / prcdcie,
        LIMITE_RC_100 = (LIMITE_RC_100 * 100) / prcdcie,
        SMP_PE_100_IND = (SMP_PE_100_IND * 100) / prcdcie,
        SMP_RD_100_IND = (SMP_RD_100_IND * 100) / prcdcie,
        PERTE_EXP_100 = (PERTE_EXP_100 * 100) / prcdcie,
        RISQUE_DIRECT_100 = (RISQUE_DIRECT_100 * 100) / prcdcie,
        SMP_100 = (SMP_100 * 100) / prcdcie,
        LCI_100 = (LCI_100 * 100) / prcdcie,
        SMP_PE_100 = (SMP_PE_100 * 100) / prcdcie,
        SMP_RD_100 = (SMP_RD_100 * 100) / prcdcie'''
}

# Business rules
BUSINESS_RULES = {
    'SMP_completion': '''SET
        SMP_100_IND = MAX(SMP_100_IND, SUM(SMP_PE_100_IND, SMP_RD_100_IND)),
        SMP_100 = MAX(SMP_100, SUM(SMP_PE_100, SMP_RD_100))''',
    'RC_limit': 'SET LIMITE_RC_100 = MAX(LIMITE_RC_100_PAR_SIN, LIMITE_RC_100_PAR_AN)'
}
```

---

## 6. PTF_MVTS_CONSOLIDATION_MACRO.sas

### 6.1. CUBE.IRD_RISK_Q46_&vision / Q45 / QAN

```python
# Version check
VERSION_CHECK = {
    'condition': '&vision. >= 202210',
    'if_true': 'USE CUBE.IRD_RISK_*_&vision.',
    'if_false': 'USE RISK_REF.ird_risk_*_202210'
}

# Datetime conversion
DATETIME_CONVERSION = {
    'DTOUCHAN_RISK': 'datepart(DTOUCHAN)',
    'DTRECTRX_RISK': 'datepart(DTRECTRX)',
    'DTREFFIN_RISK': 'datepart(DTREFFIN)',  # Q46, Q45
    'DTRCPPR_RISK': 'datepart(DTRCPPR)'  # QAN only
}

# Join conditions
JOIN = {
    'type': 'LEFT JOIN',
    'on': 'a.nopol = b.nopol'
}

# Field selection and rename
SELECT_Q46_Q45 = {
    'NOPOL': 'NOPOL',
    'DTOUCHAN': 'DTOUCHAN_RISK',
    'DTRECTRX': 'DTRECTRX_RISK',
    'DTREFFIN': 'DTREFFIN_RISK',
    'CTPRVTRV': 'CTPRVTRV_RISK',
    'CTDEFTRA': 'CTDEFTRA_RISK',
    'LBNATTRV': 'LBNATTRV_RISK',
    'LBDSTCSC': 'LBDSTCSC_RISK'
}

SELECT_QAN = {
    'NOPOL': 'NOPOL',
    'DTOUCHAN': 'DTOUCHAN_RISK',
    'DTRCPPR': 'DTRCPPR_RISK',
    'CTPRVTRV': 'CTPRVTRV_RISK',
    'CTDEFTRA': 'CTDEFTRA_RISK',
    'LBNATTRV': 'LBNATTRV_RISK',
    'DSTCSC': 'LBDSTCSC_RISK'
}

# Data merging logic (fill missing values)
MERGE_LOGIC = {
    'DTOUCHAN': 'if missing(DTOUCHAN) then DTOUCHAN=DTOUCHAN_RISK',
    'DTRECTRX': 'if missing(DTRECTRX) then DTRECTRX=DTRECTRX_RISK',
    'DTRCPPR': 'if missing(DTRCPPR) then DTRCPPR=DTRCPPR_RISK',
    'CTPRVTRV': 'if missing(CTPRVTRV) then CTPRVTRV=CTPRVTRV_RISK',
    'CTDEFTRA': 'if missing(CTDEFTRA) then CTDEFTRA=CTDEFTRA_RISK',
    'LBNATTRV': 'if missing(LBNATTRV) then LBNATTRV=LBNATTRV_RISK',
    'DSTCSC': 'if missing(DSTCSC) then DSTCSC=LBDSTCSC_RISK',
    'DTREFFIN': 'DTREFFIN=DTREFFIN_RISK (always override)'
}

# Data quality adjustment
DATA_QUALITY = {
    'DTRCPPR_fallback': 'if missing(DTRCPPR) and not missing(DTREFFIN) then DTRCPPR=DTREFFIN'
}
```

### 6.2. CLIENT1.CLACENT1 / CLIENT3.CLACENT3

```python
# Join conditions
JOIN = {
    'type': 'LEFT JOIN',
    'on': 'a.noclt = c1.noclt OR a.noclt = c3.noclt'
}

# Selected columns with coalesce
SELECT = {
    'cdsiret': 'coalesce(c1.cdsiret, c3.cdsiret)',
    'cdsiren': 'coalesce(c1.cdsiren, c3.cdsiren)',
    'CDNAF': 'coalesce(c1.CDNAF, c3.CDNAF) AS CDNAF03_CLI'
}
```

### 6.3. BINSEE.HISTO_NOTE_RISQUE

```python
# Join conditions
JOIN = {
    'type': 'LEFT JOIN',
    'on': [
        'a.cdsiren = t2.cdsiren',
        't2.dtdeb_valid <= "&dtfinmn"d',
        't2.dtfin_valid >= "&dtfinmn"d'
    ]
}

# Selected column
SELECT = {
    'note_euler': 'case when t2.cdnote in ("00") then "" else t2.cdnote end'
}
```

### 6.4. W6.BASECLI_INV

```python
# File decompression
PRE_READ = {
    'command': 'gunzip &vue./basecli_inv.sas7bdat.gz'
}

# Join conditions
JOIN = {
    'type': 'LEFT JOIN',
    'on': 'PTF.NOCLT = W6.NOCLT'
}

# Selected column
SELECT = {
    'CDNAF08_W6': 'W6.CDAPET'
}

# Post-read compression
POST_READ = {
    'command': 'gzip &vue./basecli_inv.sas7bdat'
}

# Data quality
CLEANUP = {
    'CDNAF03_CLI': 'if CDNAF03_CLI in ("00" "000Z" "9999") then CDNAF03_CLI=""',
    'CDNAF08_W6': 'if CDNAF08_W6 in ("0000Z") then CDNAF08_W6=""'
}
```

### 6.5. DEST.DO_DEST202110

```python
# Join conditions
JOIN = {
    'type': 'LEFT JOIN',
    'on': 't1.NOPOL = t2.NOPOL'
}

# Selected column
SELECT = ['DESTINAT']

# Additional business rules
BUSINESS_RULES = {
    'condition': 'segment2="Chantiers" and missing(DESTINAT)',
    'Habitation': '''if (
        PRXMATCH("/(HABIT)/", UPCASE(DSTCSC)) OR PRXMATCH("/(HABIT)/", UPCASE(LBNATTRV))
        OR PRXMATCH("/(LOG)/", UPCASE(DSTCSC)) OR PRXMATCH("/(LOG)/", UPCASE(LBNATTRV))
        OR PRXMATCH("/(LGT)/", UPCASE(DSTCSC)) OR PRXMATCH("/(LGT)/", UPCASE(LBNATTRV))
        OR PRXMATCH("/(MAIS)/", UPCASE(DSTCSC)) OR PRXMATCH("/(MAIS)/", UPCASE(LBNATTRV))
        OR PRXMATCH("/(APPA)/", UPCASE(DSTCSC)) OR PRXMATCH("/(APPA)/", UPCASE(LBNATTRV))
        OR PRXMATCH("/(VILLA)/", UPCASE(DSTCSC)) OR PRXMATCH("/(VILLA)/", UPCASE(LBNATTRV))
        OR PRXMATCH("/(INDIV)/", UPCASE(DSTCSC)) OR PRXMATCH("/(INDIV)/", UPCASE(LBNATTRV))
    ) THEN DESTINAT="Habitation"''',
    'Habitation_codes': 'if DSTCSC in("01","02","03","04","1","2","3","4","22") then DESTINAT="Habitation"',
    'Autres': 'ELSE DESTINAT="Autres"'
}
```

### 6.6. IPFSPE*.IPFM0024 / IPFM63 / IPFM99

```python
# Existence check
EXISTENCE_CHECK = {
    'function': '%sysfunc(exist(IPFSPE1.IPFM0024))',
    'action_if_false': 'Skip this table'
}

# IPFM0024
SELECT_IPFM0024 = {
    'columns': ['NOPOL', 'NOINT', 'CDPROD', 'CDACTPRF01', 'CDACTPRF02'],
    'cdpole': '"1" for IPFSPE1, "3" for IPFSPE3',
    'renames': {
        'CDACTPRF01': 'CDACTCONST',
        'CDACTPRF02': 'CDACTCONST2'
    },
    'defaults': {
        'CDNAF': '""',
        'MTCA_RIS': '.'
    }
}

# IPFM63
SELECT_IPFM63 = {
    'columns': ['NOPOL', 'NOINT', 'CDPROD', 'ACTPRIN', 'ACTSEC1', 'CDNAF', 'MTCA1'],
    'cdpole': '"1" for IPFSPE1, "3" for IPFSPE3',
    'renames': {
        'ACTPRIN': 'CDACTCONST',
        'ACTSEC1': 'CDACTCONST2',
        'MTCA1': 'MTCA_RIS'
    }
}

# IPFM99
SELECT_IPFM99 = {
    'columns': ['NOPOL', 'NOINT', 'CDPROD', 'CDACPR1', 'CDACPR2', 'MTCA'],
    'cdpole': '"1" for IPFSPE1, "3" for IPFSPE3',
    'transforms': {
        'CDACTCONST': 'substr(CDACPR1, 1, 4)',
        'CDACTCONST2': 'CDACPR2',
        'MTCA_RIS': 'MTCA'
    },
    'defaults': {
        'CDNAF': '""'
    }
}

# Union all tables
UNION = {
    'method': 'OUTER UNION CORR',
    'cleanup': 'if missing(nopol) then delete'
}

# Join with main table
JOIN = {
    'type': 'LEFT JOIN',
    'on': ['t1.NOPOL=t3.NOPOL', 't1.CDPROD=t3.CDPROD']
}

# Field priority
FIELD_PRIORITY = {
    'ACTPRIN': 'coalesce(t3.CDACTCONST, t1.ACTPRIN)',
    'CDNAF': 'CASE WHEN t3.CDNAF ne "" then t3.CDNAF ELSE t1.CDNAF END'
}

# Type of activity calculation
TYPE_ACT = {
    'Multi': 'if not missing(CDACTCONST2) then TypeAct="Multi"',
    'Mono': 'else TypeAct="Mono"'
}
```

### 6.7. ISIC.ISIC_LG_202306

```python
# Join conditions
JOIN = {
    'type': 'LEFT JOIN',
    'on': 't2.ISIC_Local = t1.ISIC_CODE'
}

# Selected column
SELECT = {
    'ISIC_CODE_GBL': 't2.ISIC_Global'
}

# ISIC code corrections (hardcoded fixes)
CORRECTIONS = {
    '22000': 'if ISIC_CODE="22000" then ISIC_CODE_GBL="022000"',
    ' 22000': 'if ISIC_CODE=" 22000" then ISIC_CODE_GBL="022000"',
    '24021': 'if ISIC_CODE="24021" then ISIC_CODE_GBL="024000"',
    ' 24021': 'if ISIC_CODE=" 24021" then ISIC_CODE_GBL="024000"',
    '242025': 'if ISIC_CODE="242025" then ISIC_CODE_GBL="242005"',
    '329020': 'if ISIC_CODE="329020" then ISIC_CODE_GBL="329000"',
    '731024': 'if ISIC_CODE="731024" then ISIC_CODE_GBL="731000"',
    '81020': 'if ISIC_CODE="81020" then ISIC_CODE_GBL="081000"',
    ' 81020': 'if ISIC_CODE=" 81020" then ISIC_CODE_GBL="081000"',
    '81023': 'if ISIC_CODE="81023" then ISIC_CODE_GBL="081000"',
    ' 81023': 'if ISIC_CODE=" 81023" then ISIC_CODE_GBL="081000"',
    '981020': 'if ISIC_CODE="981020" then ISIC_CODE_GBL="981000"'
}

# Special business flags
SPECIAL_FLAGS = {
    'TOP_BERLIOZ': 'if NOINT="4A5766" then TOP_BERLIOZ=1',
    'TOP_PARTENARIAT': 'if NOINT in ("4A6160","4A6947","4A6956") then TOP_PARTENARIAT=1'
}
```

---

## 7. PTF_MVTS_CHARG_DATARISK_ONEBI_BIFR.sas

### 7.1. RISK.IRD_RISK_Q46/Q45/QAN_&vision

```python
# Version check
VERSION_CHECK = {
    'condition': '&vision >= 202210',
    'action_if_false': 'Skip - do not load'
}

# Remote connection
REMOTE = {
    'server': 'biaa-sg-prod.srv.allianz',
    'port': 7556,
    'encoding': 'utf-8'
}

# Library path
LIBRARY = {
    'path': '/var/opt/data/data_project/sas94/bifr/azfronebi/data/shared_lib/IMS_CONTRAT/ird_risk/archives_mensuelles'
}

# Reading options
READ_OPTIONS = {
    'encoding': 'latin9'  # Force latin9 despite UTF-8 source
}

# Character encoding cleanup (remove accented characters)
ENCODING_CLEANUP = {
    'method': 'array allchar{*} _CHARACTER_',
    'replacements': {
        'é': '""',
        'è': '""',
        'ê': '""',
        'à': '""',
        'â': '""',
        'ù': '""',
        'û': '""',
        'ô': '""',
        'ç': '""',
        'î': '""',
        '°': '""'
    }
}

# Download to local
DOWNLOAD = {
    'method': 'proc download',
    'output_encoding': 'latin9'
}
```

---

## 8. REF_segmentation_azec.sas

### 8.1. SAS_C.LOB

```python
# Filter
WHERE = {
    'cmarch': 'IN ("6")'  # Construction market only
}
```

### 8.2. SAS_C.PTGST

```python
# Keep columns
KEEP = ['PTGST', 'REGION', 'P_Num']

# Deduplication
DEDUPLICATE_BY = ['PTGST']
```

### 8.3. GARANTCU.GARANTCU

```python
# Filter
WHERE = {
    'branche': 'IN ("CO","RT")'  # Construction and TRC branches
}

# Keep columns
KEEP = ['POLICE', 'garantie', 'branche']

# Processing logic (creation of typmarc fields)
PROCESSING = {
    'typmarc4': 'IF garantie = "CNAA" THEN typmarc4 = "01 02 06"',
    'typmarc5': 'IF garantie = "CNAP" THEN typmarc5 = "04 "',
    'typmarc6': 'IF garantie = "CNPR" THEN typmarc6 = "08 09 10"'
}

# Deduplication
DEDUPLICATE_BY = ['POLICE']
```

### 8.4. CONSTRCU.CONSTRCU

```python
# Selected columns
KEEP = ['POLICE', 'PRODUIT', 'typmarc1', 'ltypmar1', 'formule', 'nat_cnt']

# Filter (after join with LOB)
WHERE = {
    'CMARCH': 'IN ("6")'
}

# Deduplication
DEDUPLICATE_BY = ['POLICE']

# Activity field creation (based on typmarc1 and formule)
ACTIVITY_RULES = {
    'RBA_RCD': {
        'ARTISAN': 'typmarc1 = "01"',
        'ENTREPRISE': 'typmarc1 IN ("02" "03" "04") OR (typmarc1 = " " AND formule IN ("DA" "DB" "DC"))',
        'M. OEUVRE': 'typmarc1 IN ("05") OR (typmarc1 = " " AND formule = "DD")',
        'FABRICANT': 'typmarc1 IN ("06" "14") OR (typmarc1 = " " AND formule = "DE")',
        'NEGOCIANT': 'typmarc1 IN ("07") (RBA only)',
        'PROMOTEUR': 'typmarc1 IN ("08")',
        'M. OUVRAGE': 'typmarc1 IN ("09")',
        'MARCHAND': 'typmarc1 IN ("10")',
        'M. OEUVRE G.C': 'typmarc1 IN ("12" "13")'
    },
    'DPC': {
        'COMP. GROUPE': 'nat_cnt = "01"',
        'PUC BATIMENT': 'nat_cnt = "02"',
        'PUC G.C. ': 'nat_cnt = "03"',
        'GLOBAL CHANT': 'nat_cnt = "04"',
        'DEC G.C. ': 'nat_cnt = "05"',
        'DIVERS ': 'nat_cnt = "06"',
        'DEC BATIMENT': 'nat_cnt = "07"'
    }
}
```

### 8.5. RISTECCU.RISTECCU

```python
# Selected columns
KEEP = ['POLICE', 'PRODUIT', 'formule']

# Filter
WHERE = {
    'CMARCH': 'IN ("6")'
}
```

### 8.6. POLIC_CU.POLIC_CU

```python
# Filter
WHERE = {
    'CMARCH': 'IN ("6")'
}

# Calculated fields
CALCULATED = {
    'revisable': 'IF INDREGUL IN ("O") THEN revisable = "oui" ELSE revisable = "non"',
    'PTGST': 'poingest'
}

# Deduplication
DEDUPLICATE_BY = ['POLICE']
```

### 8.7. SAS_C.TYPRD_2

```python
# Filter
WHERE = {
    'activite': 'NOT IN ("", "DOMMAGES OUVRAGES", "RC ENTREPRISES DE CONSTRUCTION", "RC DECENNALE", "TOUS RISQUES CHANTIERS")'
}

# Deduplication
DEDUPLICATE_BY = ['activite']
```

### 8.8. CONSTRCU_AZEC (Final Output)

```python
# Type_Produit fallback rules
TYPE_PRODUIT_FALLBACK = {
    'TRC': 'IF LSSSEG IN ("TOUS RISQUES CHANTIERS") AND Type_Produit missing',
    'DO': 'IF LSSSEG IN ("DOMMAGES OUVRAGES") AND Type_Produit missing',
    'Entreprises': 'IF PRODUIT in ("RCC")',
    'Autres': 'DEFAULT if Type_Produit still missing'
}

# CSSSEG adjustment
CSSSEG_ADJUSTMENT = {
    'cssseg_7': 'IF LSSSEG = "RC DECENNALE" AND Type_Produit IN ("Artisans") THEN cssseg = 7'
}

# Keep columns
KEEP = ['POLICE', 'CDPROD', 'SEGMENT', 'TYPE_PRODUIT']

# Deduplication
DEDUPLICATE_BY = ['POLICE']
```

---

## 9. Excel Input Files

### 9.1. TDB_PRIMES_&vision..XLS

```python
# File check
FILE_CHECK = {
    'function': '%sysfunc(fileexist(...))',
    'action_if_false': 'Skip recette'
}

# Import options
IMPORT = {
    'method': 'proc import',
    'dbms': 'XLS',
    'sheet': '"SYNTHESE yc BLZ hs MAF"'
}

# Column extraction (dynamic)
COLUMN_EXTRACTION = {
    'method': 'PROC CONTENTS to get column names by position',
    'col1': 'varnum = 1 -> LoB',
    'col3': 'varnum = 3 -> Primes_emises_X',
    'col6': 'varnum = 6 -> Primes_emises_N'
}

# Data cleanup
CLEANUP = {
    'delete_blanks': 'if (Primes_emises_X="" or LoB= "") then delete'
}

# Calculated fields
CALCULATED = {
    'cdpole': 'First line by LoB = "1", else = "3"',
    'cmarch_cseg': 'if LoB in ("CONSTRUCTION") then cmarch="6", cseg="2"',
    'amounts': 'Primes_emises_X/N = round(value * 1000000, 1)'
}

# Final filter
FILTER = {
    'remove_empty': 'if cmarch="" or cdpole="" then delete'
}
```

### 9.2. TDB_PTF_MVTS_&vision..XLS

```python
# Same file check as above

# Import options
IMPORT = {
    'method': 'proc import',
    'dbms': 'XLS',
    'sheets': ['"Agent"', '"Courtage"']
}

# Calculated fields
CALCULATED = {
    'Prime_resil': 'Nbre_resil * "Prime moyenne sorties N"n',
    'Prime_AFN': 'Nbre_AFN * "Prime moyenne entrées N"n',
    'Pri_moy_Ptf_act': '"Prime moyenne clôture N"n',
    'Pri_moy_an_act': '"Prime moyenne entrées N"n',
    'Pri_moy_res_act': '"Prime moyenne sorties N"n'
}

# Market identification
MARKET_ID = {
    'CONSTRUCTION': 'if LoB in ("TOTAL CONSTRUCTION") then cmarch="6", cseg="2"'
}
```

---

## General Processing Notes

### Date Macro Variables
All programs use these date calculations:

```python
DATE_SETUP = {
    'Date': 'mdy(substr("&VISION.", 5, 2), 10, substr("&VISION.", 1, 4))',
    'Annee': 'put(year(date), z4.)',
    'Mois': 'put(month(date), z2.)',
    'Dtrefn': 'End of month date',
    'Dtrefn1': 'End of month date for previous year same month',
    'DTDEB_AN': 'mdy(1, 1, Annee) - Start of year',
    'DTFIN_AN': 'mdy(1, 1, Annee + 1) - 1 - End of year',
    'DTFIN/dtfinmn': 'End of month for vision',
    'dtfinmn1': 'End of previous month',
    'dtdebn': 'Start of year'
}
```

### Common Filters Pattern

```python
CONSTRUCTION_MARKET_FILTER = {
    'cmarch': '= "6"',
    'cseg/csegt': '= "2"'
}

EXCLUDED_INTERMEDIARIES = [
    'H90061', '482001', '489090', '102030', 'H90036', 'H90059',
    'H99045', 'H99059', '5B2000', '446000', '5R0001', '446118',
    '4F1004', '4A1400', '4A1500', '4A1600', '4A1700', '4A1800',
    '4A1900', '4L1010'
]

EXCLUDED_PRODUCTS = ['01073']
```

### Version-Dependent Logic Pattern

```python
VERSION_PATTERN = {
    'derniere_version': 'Automatically select most recent table <= vision',
    'fixed_reference': 'Use specific reference version (e.g., 202210, 202305)',
    'conditional': 'IF vision >= threshold THEN use_current ELSE use_reference'
}
```

---

## End of Document
