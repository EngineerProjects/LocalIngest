# SAS Project - Entry Tables and Raw Columns Analysis

This document lists all entry tables (data sources read from external systems) found in the SAS project, along with their raw column names as they appear in the source data (before any transformation, filtering, or renaming).

## Summary

- **Total SAS Files Analyzed**: 19
- **Entry Tables Identified**: Multiple sources from IMS, AZEC legacy, One BI, and reference data systems

---

## 1. PTF_MVTS_AZEC_MACRO.sas

### Input Tables:

#### 1.1. POLIC_CU.POLIC_CU (AZEC Policy Data)
**Libname**: POLIC_CU
**Description**: Main AZEC policy data

**Raw Columns**:
- POLICE
- DATFIN
- DATRESIL
- INTERMED
- POINGEST
- CODECOAS
- DATAFN
- ETATPOL
- DUREE
- FINPOL
- DATTERME
- DATEXPIR
- PRODUIT
- EFFETPOL
- PRIME
- PARTBRUT
- CPCUA
- NOMCLI
- MOTIFRES
- ORIGRES
- TYPCONTR
- RMPLCANT
- GESTSIT
- ECHEANMM
- ECHEANJJ
- INDREGUL
- CDNAF
- CDNAF03_CLI
- CDNAF08_W6
- CDNATP (calculated in code)

#### 1.2. CAPITXCU.CAPITXCU (AZEC Capital Data)
**Libname**: CAPITXCU
**Description**: AZEC capital data for SMP and LCI

**Raw Columns**:
- POLICE
- PRODUIT
- SMP_SRE
- BRCH_REA
- CAPX_100
- CAPX_CUA

#### 1.3. INCENDCU.INCENDCU (Fire Insurance Data)
**Libname**: INCENDCU
**Description**: Fire insurance data including loss of profit and direct risk

**Raw Columns**:
- POLICE
- PRODUIT
- COD_NAF
- COD_TRE
- MT_BASPE (Perte d'Exploitation base amount)
- MT_BASDI (Risque Direct base amount)

#### 1.4. RCENTCU.RCENTCU (RC Enterprise Data)
**Libname**: RCENTCU
**Description**: Civil Liability for Enterprises

**Raw Columns**:
- POLICE
- COD_NAF
- FORMULE
- FORMULE2
- FORMULE3
- FORMULE4

#### 1.5. RISTECCU.RISTECCU (Professional Risk Data)
**Libname**: RISTECCU
**Description**: Professional risks

**Raw Columns**:
- POLICE
- COD_NAF
- FORMULE
- FORMULE2
- FORMULE3
- FORMULE4

#### 1.6. MULPROCU.MULPROCU (Multi-risk Data)
**Libname**: MULPROCU
**Description**: Multi-risk professional data including turnover

**Raw Columns**:
- POLICE
- CHIFFAFF (Turnover - Chiffre d'affaires)

#### 1.7. MPACU.MPACU (MPA Data)
**Libname**: MPACU
**Description**: MPA policy data

**Raw Columns**:
- POLICE
- COD_NAF

#### 1.8. CONSTRCU.CONSTRCU (Construction Site Data)
**Libname**: CONSTRCU
**Description**: Construction site specific data

**Raw Columns**:
- POLICE
- PRODUIT
- DATOUVCH (Site opening date)
- LDESTLOC
- MNT_GLOB (Global amount)
- DATRECEP (Reception date)
- DEST_LOC (Destination code)
- DATFINCH (Site end date)
- LQUALITE (Quality label)

#### 1.9. NAF_2008.IRD_SUIVI_ENGAGEMENTS_&vision (NAF 2008 Tracking)
**Libname**: NAF_2008
**Description**: Engagement tracking with NAF 2008 codes (for vision >= 202103)

**Raw Columns**:
- NOPOL
- CDPROD
- CDNAF08
- CDISIC

#### 1.10. PT_GEST.PTGST_* (Management Point Reference)
**Libname**: PT_GEST
**Description**: Management point reference data

**Raw Columns**:
- PTGST
- UPPER_MID

---

## 2. CODIFICATION_ISIC_CONSTRUCTION.sas

### Reference Tables:

#### 2.1. REF_ISIC.MAPPING_CDNAF2003_ISIC_*
**Libname**: REF_ISIC
**Description**: NAF 2003 to ISIC code mapping

**Raw Columns**:
- CDNAF_2003
- ISIC_CODE

#### 2.2. REF_ISIC.MAPPING_CDNAF2008_ISIC_*
**Libname**: REF_ISIC
**Description**: NAF 2008 to ISIC code mapping

**Raw Columns**:
- CDNAF_2008
- ISIC_CODE

#### 2.3. ISIC_CST.MAPPING_ISIC_CONST_ACT_*
**Libname**: ISIC_CST
**Description**: ISIC mapping for construction activity contracts

**Raw Columns**:
- ACTPRIN (Principal activity code)
- CDNAF08
- CDTRE
- CDNAF03
- CDISIC

#### 2.4. ISIC_CST.MAPPING_ISIC_CONST_CHT_*
**Libname**: ISIC_CST
**Description**: ISIC mapping for construction sites

**Raw Columns**:
- DESTI_ISIC (Destination ISIC)
- CDNAF08
- CDTRE
- CDNAF03
- CDISIC

#### 2.5. REF_ISIC.TABLE_ISIC_TRE_NAF_*
**Libname**: REF_ISIC
**Description**: ISIC reference table with hazard grades

**Raw Columns**:
- ISIC_CODE
- HAZARD_GRADES_FIRE
- HAZARD_GRADES_BI
- HAZARD_GRADES_RCA
- HAZARD_GRADES_RCE
- HAZARD_GRADES_TRC
- HAZARD_GRADES_RCD
- HAZARD_GRADES_DO

---

## 3. EMISSIONS_RUN.sas

### Input Tables:

#### 3.1. PRM.RF_FR1_PRM_DTL_MIDCORP_M (One BI Premium Data)
**Libname**: PRM (Remote - One BI)
**Description**: Premium detail data from One BI for midcorp

**Raw Columns**:
- CD_NIV_2_STC (Distribution channel level 2)
- CD_INT_STC (Intermediary code)
- NU_CNT_PRM (Policy number)
- CD_PRD_PRM (Product code)
- CD_STATU_CTS (Status code)
- DT_CPTA_CTS (Accounting date)
- DT_EMIS_CTS (Emission date)
- DT_ANNU_CTS (Cancellation date)
- MT_HT_CTS (Excluding tax amount)
- MT_CMS_CTS (Commission amount)
- CD_CAT_MIN (Minimum category code)
- CD_GAR_PRINC (Main guarantee code)
- CD_GAR_PROSPCTIV (Prospective guarantee code)
- NU_EX_RATT_CTS (Exercise attachment number)
- CD_MARCHE (Market code)

#### 3.2. SEG.SEGMENTPRDT_&vision (Product Segmentation)
**Libname**: SEG
**Description**: Product segmentation reference

**Raw Columns**:
- CPROD (Product code)
- CDPOLE (Distribution channel)
- CMARCH (Market code)
- CSEG (Segment code)
- CSSSEG (Sub-segment code)

---

## 4. EMISSIONS_RUN_RECETTE.sas

### Input Files:

#### 4.1. TDB_PRIMES_&vision..XLS (Finance Reference - Excel)
**File Path**: `/sasprod/produits/SASEnterpriseBIServer/segrac/METGTECH/CONSTRUCTION/Recette/Input/TDB_PRIMES_&vision..XLS`
**Sheet**: "SYNTHESE yc BLZ hs MAF"
**Description**: Finance reference data for emissions quality control

**Raw Columns** (Excel - dynamic column names):
- Column 1 (LoB)
- Column 3 (Primes_emises_X)
- Column 6 (Primes_emises_N)

#### 4.2. CUBE.PRIMES_EMISES&vision._POL_GARP (Cube Emissions)
**Libname**: CUBE
**Description**: Cube emissions data with guarantee detail

**Raw Columns**:
- VISION
- CDPOLE
- CDPROD
- NOPOL
- NOINT
- CGARP
- CMARCH
- CSEG
- CSSSEG
- CD_CAT_MIN
- PRIMES_X
- PRIMES_N
- MTCOM_X

---

## 5. PTF_MVTS_AZ_MACRO.sas

### Input Tables:

#### 5.1. PTF16.IPF (Portfolio Agent)
**Libname**: PTF16 (STP3 Remote)
**Description**: Portfolio data for Agent channel (CDPOLE=1)

**Raw Columns**:
- CDPROD
- NOPOL
- NOCLT
- NMCLT
- NMACTA
- POSACTA
- RUEACTA
- CEDIACTA
- NOINT
- DTCREPOL
- CDNATP
- TXCEDE
- PTGST
- DTEFFAN
- DTTRAAN
- DTRESILP
- DTTRAAR
- CDTYPLI1, CDTYPLI2, CDTYPLI3
- DTTYPLI1, DTTYPLI2, DTTYPLI3
- CMARCH
- CDSITP
- MTPRPRTO
- PRCDCIE
- CSEGT
- CSSEGT
- CDRI
- CDCIEORI
- CDSITMGR
- CDGECENT
- CDMOTRES
- CDPOLRVI
- NOPOLLI1
- CDCASRES
- DTECHANN
- CDTPCOA
- CDPOLQPL
- CDGREV
- CDFRACT
- QUARISQ
- NMRISQ
- NMSRISQ
- RESRISQ
- RUERISQ
- LIDIRISQ
- POSRISQ
- VILRISQ
- CDREG
- MTCAPI1 through MTCAPI14 (14 capital amount fields)
- LBCAPI1 through LBCAPI14 (14 capital label fields)
- CTDEFTRA
- CTPRVTRV
- DTOUCHAN
- DTRCPPR
- DTRECTRX
- DTRCPRE
- LBNATTRV
- DSTCSC
- LBQLTSOU
- ACTPRIN
- MTSMPR
- CDNAF
- CDTRE
- CDACTPRO
- FNCMACA (MTCA - Turnover)
- MTCAF
- CTDUREE
- TYDRIS1
- OPAPOFFR

#### 5.2. PTF36.IPF (Portfolio Courtage)
**Libname**: PTF36 (STP3 Remote)
**Description**: Portfolio data for Courtage channel (CDPOLE=3)

**Raw Columns**: Same as PTF16.IPF

#### 5.3. PTF16a.IPFM99 (Special Product 01099 - Agent)
**Libname**: PTF16a (STP3 Remote)
**Description**: Turnover data for product 01099 - Agent

**Raw Columns**:
- CDPOLE
- CDPROD
- NOPOL
- NOINT
- MTCA (Total turnover)
- MTCAENP (Turnover in progress)
- MTCASST (Turnover submitted)
- MTCAVNT (Sales turnover)

#### 5.4. PTF36a.IPFM99 (Special Product 01099 - Courtage)
**Libname**: PTF36a (STP3 Remote)
**Description**: Turnover data for product 01099 - Courtage

**Raw Columns**: Same as PTF16a.IPFM99

#### 5.5. SEGMprdt.PRDPFA1 (Segmentation Agent)
**Libname**: SEGMprdt (STP3 Remote)
**Description**: Product segmentation for Agent channel

**Raw Columns**:
- CPROD
- CMARCH
- LMARCH
- CSEG
- LSEG
- CSSSEG
- LSSSEG
- LPROD

#### 5.6. SEGMprdt.PRDPFA3 (Segmentation Courtage)
**Libname**: SEGMprdt (STP3 Remote)
**Description**: Product segmentation for Courtage channel

**Raw Columns**: Same as PRDPFA1

#### 5.7. PRDCAP.PRDCAP (Product Catalog)
**Libname**: PRDCAP (STP3 Remote)
**Description**: Product catalog with labels

**Raw Columns**:
- CDPROD
- LBTPROD (Product label)

---

## 6. CAPITAUX_AZ_MACRO.sas

### Input Tables:

#### 6.1. PTF16.IPF (Portfolio Agent - Capital Focus)
**Libname**: PTF16 (STP3 Remote)
**Description**: Portfolio data for capital calculations - Agent

**Raw Columns** (Capital-specific):
- CDPROD
- NOPOL
- NMCLT
- NOINT
- DTCREPOL
- CDNATP
- TXCEDE
- PTGST
- DTRESILP
- CMARCH
- CSEGT
- CSSEGT
- CDSITP
- MTPRPRTO
- PRCDCIE
- CDGECENT
- DTEFSITT (Contract start date for indexation)
- CDMOTRES
- CDCASRES
- DTECHANN
- CDTPCOA
- CDPOLQPL
- MTCAPI1 through MTCAPI14
- LBCAPI1 through LBCAPI14
- CDPRVB1 through CDPRVB14 (Construction nature codes for indexation)
- PRPRVC1 through PRPRVC14 (Evolution coefficients for indexation)

#### 6.2. PTF36.IPF (Portfolio Courtage - Capital Focus)
**Libname**: PTF36 (STP3 Remote)
**Description**: Portfolio data for capital calculations - Courtage

**Raw Columns**: Same as PTF16.IPF (capital-specific)

---

## 7. CAPITAUX_AZEC_MACRO.sas

### Input Tables:

#### 7.1. CAPITXCU.CAPITXCU (AZEC Capital Data - Duplicate entry)
**Libname**: CAPITXCU
**Description**: AZEC capital data

**Raw Columns**: Already listed in section 1.2

#### 7.2. INCENDCU.INCENDCU (Fire Data - Duplicate entry)
**Libname**: INCENDCU
**Description**: Fire insurance data

**Raw Columns**: Already listed in section 1.3

#### 7.3. SEG.SEGMENTPRDT_&vision (Segmentation - Duplicate entry)
**Libname**: SEG
**Description**: Product segmentation

**Raw Columns**: Already listed in section 3.2

---

## 8. PTF_MVTS_CONSOLIDATION_MACRO.sas

### Input Tables:

#### 8.1. CUBE.IRD_RISK_Q46_&vision (RISK Questionnaire 46)
**Libname**: CUBE (or RISK_REF for vision < 202210)
**Description**: Risk questionnaire 46 data from One BI

**Raw Columns**:
- NOPOL
- DTOUCHAN (datetime)
- DTRECTRX (datetime)
- DTREFFIN (datetime)
- CTPRVTRV
- CTDEFTRA
- LBNATTRV
- LBDSTCSC

#### 8.2. CUBE.IRD_RISK_Q45_&vision (RISK Questionnaire 45)
**Libname**: CUBE (or RISK_REF for vision < 202210)
**Description**: Risk questionnaire 45 data from One BI

**Raw Columns**:
- NOPOL
- DTOUCHAN (datetime)
- DTRECTRX (datetime)
- DTREFFIN (datetime)
- CTPRVTRV
- CTDEFTRA
- LBNATTRV
- LBDSTCSC

#### 8.3. CUBE.IRD_RISK_QAN_&vision (RISK Annual Questionnaire)
**Libname**: CUBE (or RISK_REF for vision < 202210)
**Description**: Annual risk questionnaire from One BI

**Raw Columns**:
- NOPOL
- DTOUCHAN (datetime)
- DTRCPPR (datetime)
- CTPRVTRV
- CTDEFTRA
- LBNATTRV
- DSTCSC

#### 8.4. CLIENT1.CLACENT1 (Client Data Agent)
**Libname**: CLIENT1 (STP3 Remote)
**Description**: Client account data for Agent channel

**Raw Columns**:
- NOCLT
- CDSIRET
- CDSIREN
- CDNAF

#### 8.5. CLIENT3.CLACENT3 (Client Data Courtage)
**Libname**: CLIENT3 (STP3 Remote)
**Description**: Client account data for Courtage channel

**Raw Columns**:
- NOCLT
- CDSIRET
- CDSIREN
- CDNAF

#### 8.6. BINSEE.HISTO_NOTE_RISQUE (Euler Risk Rating)
**Libname**: BINSEE
**Description**: Historical risk rating from Euler

**Raw Columns**:
- CDSIREN
- CDNOTE (Risk note code)
- DTDEB_VALID (Validity start date)
- DTFIN_VALID (Validity end date)

#### 8.7. W6.BASECLI_INV (Client Base W6)
**Libname**: W6
**Description**: Cumulative client base

**Raw Columns**:
- NOCLT
- CDAPET (NAF code APE)

#### 8.8. DEST.DO_DEST202110 (Construction Site Destination)
**Libname**: DEST
**Description**: Construction site destination reference

**Raw Columns**:
- NOPOL
- DESTINAT

#### 8.9. IPFSPE1.IPFM0024 (Special Product Data Agent)
**Libname**: IPFSPE1 (STP3 Remote)
**Description**: Special product data for Agent - Product activity

**Raw Columns**:
- NOPOL
- NOINT
- CDPROD
- CDACTPRF01 (Principal activity code 1)
- CDACTPRF02 (Secondary activity code 1)

#### 8.10. IPFSPE3.IPFM0024 (Special Product Data Courtage)
**Libname**: IPFSPE3 (STP3 Remote)
**Description**: Special product data for Courtage - Product activity

**Raw Columns**: Same as IPFSPE1.IPFM0024

#### 8.11. IPFSPE1.IPFM63 (Special Product 63 Agent)
**Libname**: IPFSPE1 (STP3 Remote)
**Description**: Special product data - Professional activities

**Raw Columns**:
- NOPOL
- NOINT
- CDPROD
- ACTPRIN (Principal activity)
- ACTSEC1 (Secondary activity 1)
- CDNAF
- MTCA1 (Turnover 1)

#### 8.12. IPFSPE3.IPFM63 (Special Product 63 Courtage)
**Libname**: IPFSPE3 (STP3 Remote)
**Description**: Special product data - Professional activities

**Raw Columns**: Same as IPFSPE1.IPFM63

#### 8.13. IPFSPE1.IPFM99 (Special Product 99 Agent)
**Libname**: IPFSPE1 (STP3 Remote)
**Description**: Special product data - Generic activities

**Raw Columns**:
- NOPOL
- NOINT
- CDPROD
- CDACPR1 (Principal activity code 1)
- CDACPR2 (Secondary activity code 2)
- MTCA (Turnover)

#### 8.14. IPFSPE3.IPFM99 (Special Product 99 Courtage)
**Libname**: IPFSPE3 (STP3 Remote)
**Description**: Special product data - Generic activities

**Raw Columns**: Same as IPFSPE1.IPFM99

#### 8.15. ISIC.ISIC_LG_202306 (ISIC Local to Global Mapping)
**Libname**: ISIC
**Description**: ISIC local to global code mapping

**Raw Columns**:
- ISIC_Local
- ISIC_Global

---

## 9. PTF_MVTS_CHARG_DATARISK_ONEBI_BIFR.sas

### Input Tables:

#### 9.1. RISK.IRD_RISK_Q46_&vision (RISK Q46 from One BI)
**Libname**: RISK (Remote - One BI)
**Description**: Risk questionnaire 46 from One BI archives (vision >= 202210)

**Raw Columns**: All character columns (encoding-cleaned)

#### 9.2. RISK.IRD_RISK_Q45_&vision (RISK Q45 from One BI)
**Libname**: RISK (Remote - One BI)
**Description**: Risk questionnaire 45 from One BI archives (vision >= 202210)

**Raw Columns**: All character columns (encoding-cleaned)

#### 9.3. RISK.IRD_RISK_QAN_&vision (RISK QAN from One BI)
**Libname**: RISK (Remote - One BI)
**Description**: Annual risk questionnaire from One BI archives (vision >= 202210)

**Raw Columns**: All character columns (encoding-cleaned)

---

## 10. REF_segmentation_azec.sas

### Reference Tables:

#### 10.1. SAS_C.LOB (Line of Business)
**Libname**: SAS_C
**Description**: Product segmentation for construction market

**Raw Columns**:
- PRODUIT
- CDPROD
- CPROD
- CMARCH
- LMARCH
- LMARCH2
- CSEG
- LSEG
- LSEG2
- CSSSEG
- LSSSEG
- LSSSEG2
- LPROD
- SEGMENT

#### 10.2. SAS_C.PTGST (Management Points)
**Libname**: SAS_C
**Description**: Management point reference

**Raw Columns**:
- PTGST
- REGION
- P_NUM

#### 10.3. SAS_C.IMPORT_CATMIN (Category Minimum)
**Libname**: SAS_C
**Description**: Minimum category import

**Raw Columns**: (Not explicitly listed in code)

#### 10.4. GARANTCU.GARANTCU (AZEC Guarantees)
**Libname**: GARANTCU
**Description**: AZEC guarantee data

**Raw Columns**:
- POLICE
- GARANTIE
- BRANCHE

#### 10.5. CONSTRCU.CONSTRCU (AZEC Construction Data)
**Libname**: CONSTRCU
**Description**: AZEC construction policy data

**Raw Columns**:
- POLICE
- PRODUIT
- TYPMARC1 (Market type code 1)
- LTYPMAR1 (Market type label 1)
- FORMULE
- NAT_CNT (Contract nature)

#### 10.6. RISTECCU.RISTECCU (AZEC Professional Risk)
**Libname**: RISTECCU
**Description**: AZEC professional risk data

**Raw Columns**:
- POLICE
- PRODUIT
- FORMULE

#### 10.7. POLIC_CU.POLIC_CU (AZEC Policy)
**Libname**: POLIC_CU
**Description**: AZEC policy master data

**Raw Columns**:
- POLICE
- PRODUIT
- POINGEST
- INDREGUL

#### 10.8. SAS_C.TYPRD_2 (Product Type Mapping)
**Libname**: SAS_C
**Description**: Activity to product type mapping

**Raw Columns**:
- ACTIVITE
- TYPE_PRODUIT

---

## 11. PTF_MVTS_RUN_RECETTE.sas

### Input Files:

#### 11.1. TDB_PTF_MVTS_&vision..XLS (Finance Reference PTF/MVT - Excel)
**File Path**: `/sasprod/produits/SASEnterpriseBIServer/segrac/METGTECH/CONSTRUCTION/Recette/Input/TDB_PTF_MVTS_&vision..XLS`
**Sheets**: "Agent", "Courtage"
**Description**: Finance reference data for portfolio/movements quality control

**Raw Columns** (Excel):
- CDPOLE
- LOB
- NBRE_POL (Number of policies)
- NBRE_AFN (Number of new business)
- NBRE_RESIL (Number of terminations)
- PRIME_CLOTURE (Closing premium)
- "Prime moyenne clôture N"n
- "Prime moyenne entrées N"n
- "Prime moyenne sorties N"n

#### 11.2. CUBE.MVT_PTF&vision (Cube Portfolio Movements)
**Libname**: CUBE
**Description**: Cube portfolio and movements data

**Raw Columns**:
- VISION
- CDPOLE
- CMARCH
- CSEG
- NBAFN
- NBRES
- NBPTF
- PRIMES_AFN
- PRIMES_RES
- PRIMES_PTF

---

## 12. INDICES Library (indexation_v2.sas)

### Format Library:

#### 12.1. INDICES.NAUTIND3 (Construction Indices)
**Libname**: INDICES (STP3 Remote)
**Description**: Construction cost indices for capital indexation

**Raw Columns/Formats**:
- $INDICE format (Construction nature code + date -> index value)

---

## Additional Reference Tables

### Global References:

#### MIG_AZEC.REF_MIG_AZEC_VS_IMS
**Libname**: MIG_AZEC
**Description**: AZEC to IMS migration reference

**Raw Columns**:
- NOPOL_AZEC

#### AACPRTF.CPRODUIT
**Libname**: AACPRTF
**Description**: Product catalog with type and segment

**Raw Columns**:
- CPROD
- TYPE_PRODUIT_2
- SEGMENT
- SEGMENT_3

---

## Notes

### Date Formats
Many date fields in SAS are stored as SAS date values and may require format conversion when extracting.

### Encoding
- AZEC legacy data (POLIC_CU, CAPITXCU, etc.) is stored in LATIN9 encoding
- One BI data (RISK, PRM) is stored in UTF-8 encoding
- The project includes transcodification macros to handle encoding conversions

### Vision-Dependent Tables
Some tables are version-dependent:
- RISK tables: Use current vision for >= 202210, use reference 202210 for older visions
- NAF 2008 data: Available only for vision >= 202103
- AZEC data: Available for vision between 201211 and 202008

### Remote Libraries
Several libraries are accessed via remote SAS server connections:
- **STP3 Server** (serveur=STP3 7013): PTF16, PTF36, CLIENT1, CLIENT3, IPFSPE*, INDICES, SEGMprdt, PRDCAP
- **One BI Server** (biaa-sg-prod.srv.allianz:7556): PRM, RISK

### Indexation
Capital amounts (MTCAPI1-14) use construction cost indices based on:
- Construction nature codes (CDPRVB1-14)
- Evolution coefficients (PRPRVC1-14)
- Contract start date (DTEFSITT)
- Target indexation date (DTECHANN or custom date)

---

## Legend

- **Libname**: SAS library reference
- **Raw Columns**: Column names as they appear in the source table (before transformation)
- **Description**: Purpose and content of the table
- **Format**: Special formatting or data type information
