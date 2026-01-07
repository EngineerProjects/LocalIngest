"""
Explicit PySpark schemas for all bronze layer file groups.

Defines strict schemas to ensure correct type inference and prevent runtime errors.
All schemas use nullable=True to handle missing values gracefully.

IMPORTANT: Column names are CASE-SENSITIVE and must match EXACTLY as they appear 
in the actual CSV files. These are validated against actual data discovery.

NOTE: Schemas are OPTIONAL during the column discovery phase. When schema_name is None 
in reading_config.json, data will be read with automatic type inference, allowing you 
to discover all available columns.

SCHEMAS BASED ON:
- Column discovery from actual CSV files (2025-12-04)
- Only includes files currently available in datalake
- Column names match exact case from source data
- Only includes columns required by SAS transformation logic
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    DateType
)


# =============================================================================
# ONE BI FILES - rf_fr1_prm_dtl_midcorp_m
# File: rf_fr1_prm_dtl_midcorp_m_*.csv 🔴
# Total columns: 15 (required from SAS EMISSIONS_RUN.sas)
# =============================================================================
RF_FR1_PRM_DTL_MIDCORP_M_SCHEMA = StructType([
    StructField("CD_NIV_2_STC", StringType(), True),  # Distribution level
    StructField("CD_INT_STC", StringType(), True),  # Intermediary code
    StructField("NU_CNT_PRM", StringType(), True),  # Policy number
    StructField("CD_PRD_PRM", StringType(), True),  # Product code
    StructField("CD_STATU_CTS", StringType(), True),  # Status code
    StructField("DT_CPTA_CTS", DateType(), True),  # Accounting date
    StructField("DT_EMIS_CTS", DateType(), True),  # Emission date
    StructField("DT_ANNU_CTS", DateType(), True),  # Cancellation date
    StructField("MT_HT_CTS", DoubleType(), True),  # Premium amount (excl tax)
    StructField("MT_CMS_CTS", DoubleType(), True),  # Commission amount
    StructField("CD_CAT_MIN", StringType(), True),  # Minimum category code
    StructField("CD_GAR_PRINC", StringType(), True),  # Principal guarantee code
    StructField("CD_GAR_PROSPCTIV", StringType(), True),  # Prospective guarantee code
    StructField("NU_EX_RATT_CTS", StringType(), True),  # Attachment year
    StructField("CD_MARCHE", StringType(), True),  # Market code
])


# =============================================================================
# IMS FILES - IPFE16/IPFE36 (PTF16/PTF36)
# Files: ipf16.csv.gz, ipf36.csv.gz 🟢
# Total columns: 133 (as discovered in actual CSV)
# =============================================================================
IPF_AZ_SCHEMA = StructType([
    # Core Identifiers
    StructField("CDPROD", StringType(), True),
    StructField("NOPOL", StringType(), True),
    StructField("NOCLT", StringType(), True),
    StructField("NMCLT", StringType(), True),
    StructField("NOINT", StringType(), True),
    StructField("NMACTA", StringType(), True),
    
    # Address Fields
    StructField("POSACTA", StringType(), True),
    StructField("RUEACTA", StringType(), True),
    StructField("CEDIACTA", StringType(), True),
    
    # Key Dates
    StructField("DTCREPOL", DateType(), True),
    StructField("DTEFFAN", DateType(), True),
    StructField("DTTRAAN", DateType(), True),
    StructField("DTRESILP", DateType(), True),
    StructField("DTTRAAR", DateType(), True),
    StructField("DTTYPLI1", DateType(), True),
    StructField("DTTYPLI2", DateType(), True),
    StructField("DTTYPLI3", DateType(), True),
    StructField("DTECHANN", DateType(), True),
    StructField("DTOUCHAN", DateType(), True),
    StructField("DTRCPPR", DateType(), True),
    StructField("DTRECTRX", DateType(), True),
    StructField("DTRCPRE", DateType(), True),
    StructField("DTEFSITT", DateType(), True),
    
    # Policy Attributes
    StructField("CDNATP", StringType(), True),
    StructField("TXCEDE", DoubleType(), True),
    StructField("PTGST", StringType(), True),
    StructField("CMARCH", StringType(), True),
    StructField("CDSITP", StringType(), True),
    StructField("CSEGT", StringType(), True),
    StructField("CSSEGT", StringType(), True),
    StructField("CDRI", StringType(), True),
    
    # Type Liaison / Replacement
    StructField("CDTYPLI1", StringType(), True),
    StructField("CDTYPLI2", StringType(), True),
    StructField("CDTYPLI3", StringType(), True),
    
    # Financial Fields
    StructField("MTPRPRTO", DoubleType(), True),
    StructField("PRCDCIE", DoubleType(), True),
    StructField("MTCAF", DoubleType(), True),
    StructField("FNCMACA", DoubleType(), True),
    StructField("MTSMPR", DoubleType(), True),
    
    # Coassurance Fields
    StructField("CDPOLQPL", StringType(), True),
    StructField("CDTPCOA", StringType(), True),
    StructField("CDCIEORI", StringType(), True),
    
    # Revision Fields
    StructField("CDPOLRVI", StringType(), True),
    StructField("CDGREV", StringType(), True),
    
    # Management & Administrative
    StructField("CDSITMGR", StringType(), True),
    StructField("CDGECENT", StringType(), True),
    StructField("CDMOTRES", StringType(), True),
    StructField("NOPOLLI1", StringType(), True),
    StructField("CDCASRES", StringType(), True),
    
    # Capital Fields (14 pairs: amount + label)
    StructField("MTCAPI1", DoubleType(), True),
    StructField("MTCAPI2", DoubleType(), True),
    StructField("MTCAPI3", DoubleType(), True),
    StructField("MTCAPI4", DoubleType(), True),
    StructField("MTCAPI5", DoubleType(), True),
    StructField("MTCAPI6", DoubleType(), True),
    StructField("MTCAPI7", DoubleType(), True),
    StructField("MTCAPI8", DoubleType(), True),
    StructField("MTCAPI9", DoubleType(), True),
    StructField("MTCAPI10", DoubleType(), True),
    StructField("MTCAPI11", DoubleType(), True),
    StructField("MTCAPI12", DoubleType(), True),
    StructField("MTCAPI13", DoubleType(), True),
    StructField("MTCAPI14", DoubleType(), True),
    StructField("LBCAPI1", StringType(), True),
    StructField("LBCAPI2", StringType(), True),
    StructField("LBCAPI3", StringType(), True),
    StructField("LBCAPI4", StringType(), True),
    StructField("LBCAPI5", StringType(), True),
    StructField("LBCAPI6", StringType(), True),
    StructField("LBCAPI7", StringType(), True),
    StructField("LBCAPI8", StringType(), True),
    StructField("LBCAPI9", StringType(), True),
    StructField("LBCAPI10", StringType(), True),
    StructField("LBCAPI11", StringType(), True),
    StructField("LBCAPI12", StringType(), True),
    StructField("LBCAPI13", StringType(), True),
    StructField("LBCAPI14", StringType(), True),
    
    # Provider/Coverage Fields (14 pairs: code + rate)
    StructField("CDPRVB1", StringType(), True),
    StructField("CDPRVB2", StringType(), True),
    StructField("CDPRVB3", StringType(), True),
    StructField("CDPRVB4", StringType(), True),
    StructField("CDPRVB5", StringType(), True),
    StructField("CDPRVB6", StringType(), True),
    StructField("CDPRVB7", StringType(), True),
    StructField("CDPRVB8", StringType(), True),
    StructField("CDPRVB9", StringType(), True),
    StructField("CDPRVB10", StringType(), True),
    StructField("CDPRVB11", StringType(), True),
    StructField("CDPRVB12", StringType(), True),
    StructField("CDPRVB13", StringType(), True),
    StructField("CDPRVB14", StringType(), True),
    StructField("PRPRVC1", DoubleType(), True),
    StructField("PRPRVC2", DoubleType(), True),
    StructField("PRPRVC3", DoubleType(), True),
    StructField("PRPRVC4", DoubleType(), True),
    StructField("PRPRVC5", DoubleType(), True),
    StructField("PRPRVC6", DoubleType(), True),
    StructField("PRPRVC7", DoubleType(), True),
    StructField("PRPRVC8", DoubleType(), True),
    StructField("PRPRVC9", DoubleType(), True),
    StructField("PRPRVC10", DoubleType(), True),
    StructField("PRPRVC11", DoubleType(), True),
    StructField("PRPRVC12", DoubleType(), True),
    StructField("PRPRVC13", DoubleType(), True),
    StructField("PRPRVC14", DoubleType(), True),
    
    # Risk Fields
    StructField("CDFRACT", StringType(), True),
    StructField("QUARISQ", StringType(), True),
    StructField("NMRISQ", StringType(), True),
    StructField("NMSRISQ", StringType(), True),
    StructField("RESRISQ", StringType(), True),
    StructField("RUERISQ", StringType(), True),
    StructField("LIDIRISQ", StringType(), True),
    StructField("POSRISQ", StringType(), True),
    StructField("VILRISQ", StringType(), True),
    StructField("CDREG", StringType(), True),
    
    # NAF / Activity Codes
    StructField("CDNAF", StringType(), True),
    StructField("CDTRE", StringType(), True),
    StructField("CDACTPRO", StringType(), True),
    StructField("ACTPRIN", StringType(), True),
    
    # IRD/Transaction Fields
    StructField("tydris1", StringType(), True),  # NOTE: lowercase in actual data
    StructField("OPAPOFFR", StringType(), True),
    StructField("CTDEFTRA", StringType(), True),
    StructField("CTPRVTRV", StringType(), True),
    StructField("LBNATTRV", StringType(), True),
    StructField("DSTCSC", StringType(), True),
    StructField("LBQLTSOU", StringType(), True),
])


# =============================================================================
# IMS FILES - 3SPEIPFM99/E1SPEIPFM99 (PTF16a/PTF36a)
# Files: 3SPEIPFM99_IPF_*.csv.gz, E1SPEIPFM99_IPF_*.csv.gz 🟢
# Total columns: 9
# =============================================================================
IPFM99_AZ_SCHEMA = StructType([
    StructField("CDPROD", StringType(), True),
    StructField("NOPOL", StringType(), True),
    StructField("NOINT", StringType(), True),
    StructField("CDACPR1", StringType(), True),
    StructField("CDACPR2", StringType(), True),
    StructField("MTCA", DoubleType(), True),
    StructField("MTCAENP", DoubleType(), True),
    StructField("MTCASST", DoubleType(), True),
    StructField("MTCAVNT", DoubleType(), True),
])


# =============================================================================
# IRD FILES - Q45/Q46/QAN
# Files: ird_risk_q45_*.csv, ird_risk_q46_*.csv, ird_risk_qan_*.csv 🟢
# =============================================================================
# Q45 and Q46 share identical schema (8 fields)
IRD_RISK_COMMON_SCHEMA = StructType([
    StructField("NOPOL", StringType(), True),
    StructField("DTOUCHAN", DateType(), True),
    StructField("DTRECTRX", DateType(), True),
    StructField("DTREFFIN", DateType(), True),
    StructField("CTPRVTRV", StringType(), True),
    StructField("CTDEFTRA", StringType(), True),
    StructField("LBNATTRV", StringType(), True),
    StructField("LBDSTCSC", StringType(), True),
])

# QAN has different fields (no DTRECTRX, DTREFFIN, LBDSTCSC)
IRD_RISK_QAN_SCHEMA = StructType([
    StructField("NOPOL", StringType(), True),
    StructField("DTOUCHAN", DateType(), True),
    StructField("CTPRVTRV", StringType(), True),
    StructField("CTDEFTRA", StringType(), True),
    StructField("LBNATTRV", StringType(), True),
])


# =============================================================================
# REFERENCE FILES - INCENDCU (AZEC)
# File: incendcu.csv (in bronze/ref) 🟢
# =============================================================================
INCENDCU_SCHEMA = StructType([
    StructField("POLICE", StringType(), True),
    StructField("PRODUIT", StringType(), True),
    StructField("COD_NAF", StringType(), True),
    StructField("COD_TRE", StringType(), True),
    StructField("MT_BASPE", DoubleType(), True),
    StructField("MT_BASDI", DoubleType(), True),
])


# =============================================================================
# REFERENCE FILES - CONSTRCU (AZEC)
# File: constru.csv (in bronze/ref) 🟢
# =============================================================================
CONSTRCU_SCHEMA = StructType([
    StructField("DATFINCH", DateType(), True),
    StructField("DATOUVCH", DateType(), True),
    StructField("DATRECEP", DateType(), True),
    StructField("DEST_LOC", StringType(), True),
    StructField("FORMULE", StringType(), True),
    StructField("LDESTLOC", StringType(), True),
    StructField("LQUALITE", StringType(), True),
    StructField("LTYPMAR1", StringType(), True),
    StructField("MNT_GLOB", DoubleType(), True),
    StructField("NAT_CNT", StringType(), True),
    StructField("POLICE", StringType(), True),
    StructField("PRODUIT", StringType(), True),
    StructField("TYPMARC1", StringType(), True),
])

# =============================================================================
# REFERENCE FILES - CONSTRCU_AZEC (Product Type Classification for AZEC)
# Based on: REF_segmentation_azec.sas L341-343
# File: constrcu.csv (simplified - only 4 columns kept by SAS)
# =============================================================================
CONSTRCU_AZEC_SCHEMA = StructType([
    StructField("POLICE", StringType(), True),
    StructField("CDPROD", StringType(), True),      # Note: CDPROD not PRODUIT!
    StructField("SEGMENT", StringType(), True),
    StructField("TYPE_PRODUIT", StringType(), True),
])



# =============================================================================
# REFERENCE FILES - Product Segmentation
# Files: prdpfa1.csv, prdpfa3.csv (in bronze/ref) 🟢
# =============================================================================
SEGMPRDT_SCHEMA = StructType([
    StructField("CMARCH", StringType(), True),
    StructField("CPROD", StringType(), True),
    StructField("CSEG", StringType(), True),
    StructField("CSSSEG", StringType(), True),
    StructField("LMARCH", StringType(), True),
    StructField("LPROD", StringType(), True),
    StructField("LSEG", StringType(), True),
    StructField("LSSSEG", StringType(), True),
])


# =============================================================================
# REFERENCE FILES - Product Classification (LOB)
# File: lob.csv (in bronze/ref) 🟢
# =============================================================================
LOB_SCHEMA = StructType([
    StructField("PRODUIT", StringType(), True),
    StructField("CDPROD", StringType(), True),
    StructField("CPROD", StringType(), True),
    StructField("CMARCH", StringType(), True),
    StructField("LMARCH", StringType(), True),
    StructField("LMARCH2", StringType(), True),
    StructField("CSEG", StringType(), True),
    StructField("LSEG", StringType(), True),
    StructField("LSEG2", StringType(), True),
    StructField("CSSSEG", StringType(), True),
    StructField("LSSSEG", StringType(), True),
    StructField("LSSSEG2", StringType(), True),
    StructField("LPROD", StringType(), True),
    StructField("SEGMENT", StringType(), True),
])


# =============================================================================
# REFERENCE FILES - Product Reference (CPRODUIT)
# File: cproduit.csv (in bronze/ref) 🟢
# =============================================================================
CPRODUIT_SCHEMA = StructType([
    StructField("cprod", StringType(), True),  # NOTE: lowercase
    StructField("Type_Produit_2", StringType(), True),
    StructField("segment", StringType(), True),  # NOTE: lowercase
    StructField("Segment_3", StringType(), True),
])


# =============================================================================
# REFERENCE FILES - Guarantees
# File: garantcu.csv (in bronze/ref) 🟢
# =============================================================================
GARANTCU_SCHEMA = StructType([
    StructField("POLICE", StringType(), True),
    StructField("GARANTIE", StringType(), True),
    StructField("BRANCHE", StringType(), True),
])


# =============================================================================
# REFERENCE FILES - Category Minimum
# File: import_catmin.csv (in bronze/ref) 🟢
# =============================================================================
CATMIN_SCHEMA = StructType([
    StructField("PRODUIT", StringType(), True),
    StructField("GARANTIE", StringType(), True),
    StructField("CATMIN5", StringType(), True),
])


# =============================================================================
# AZEC REFERENCE FILES - POLIC_CU - AZEC Policy Master Data 🟢
# =============================================================================
POLIC_CU_SCHEMA = StructType([
    StructField("CODECOAS", StringType(), True),
    StructField("CPCUA", DoubleType(), True),
    StructField("DATAFN", DateType(), True),
    StructField("DATEXPIR", DateType(), True),
    StructField("DATFIN", DateType(), True),
    StructField("DATRESIL", DateType(), True),
    StructField("DATTERME", DateType(), True),
    StructField("DUREE", StringType(), True),
    StructField("ECHEANJJ", StringType(), True),
    StructField("ECHEANMM", StringType(), True),
    StructField("EFFETPOL", DateType(), True),
    StructField("ETATPOL", StringType(), True),
    StructField("FINPOL", DateType(), True),
    StructField("GESTSIT", StringType(), True),
    StructField("INDREGUL", StringType(), True),
    StructField("INTERMED", StringType(), True),
    StructField("MOTIFRES", StringType(), True),
    StructField("NOMCLI", StringType(), True),
    StructField("ORIGRES", StringType(), True),
    StructField("PARTBRUT", DoubleType(), True),
    StructField("POINGEST", StringType(), True),
    StructField("POLICE", StringType(), True),
    StructField("PRIME", DoubleType(), True),
    StructField("PRODUIT", StringType(), True),
    StructField("RMPLCANT", StringType(), True),
    StructField("TYPCONTR", StringType(), True),
])


# =============================================================================
# AZEC REFERENCE FILES - CAPITXCU - AZEC Capital Data 🟢
# =============================================================================
CAPITXCU_SCHEMA = StructType([
    StructField("POLICE", StringType(), True),
    StructField("PRODUIT", StringType(), True),
    StructField("SMP_SRE", StringType(), True),  # "LCI" or "SMP"
    StructField("BRCH_REA", StringType(), True),  # "IP0" or "ID0"
    StructField("CAPX_100", DoubleType(), True),
    StructField("CAPX_CUA", DoubleType(), True),
])


# =============================================================================
# AZEC REFERENCE FILES - FORMULE Schema (RCENTCU + RISTECCU) 🟢
# Shared by: RC Enterprise Data and Professional Risk Data
# Both tables have identical structure: POLICE, COD_NAF, FORMULE×4
# =============================================================================
AZEC_FORMULE_SCHEMA = StructType([
    StructField("POLICE", StringType(), True),
    StructField("COD_NAF", StringType(), True),
    StructField("FORMULE", StringType(), True),
    StructField("FORMULE2", StringType(), True),
    StructField("FORMULE3", StringType(), True),
    StructField("FORMULE4", StringType(), True),
])


# =============================================================================
# AZEC REFERENCE FILES - MULPROCU - Multi-risk Professional Data 🟢
# =============================================================================
MULPROCU_SCHEMA = StructType([
    StructField("POLICE", StringType(), True),
    StructField("CHIFFAFF", DoubleType(), True),  # Turnover
])


# =============================================================================
# AZEC REFERENCE FILES - MPACU - MPA Policy Data 🟢
# =============================================================================
MPACU_SCHEMA = StructType([
    StructField("POLICE", StringType(), True),
    StructField("COD_NAF", StringType(), True),
])


# =============================================================================
# CLIENT DATA - Agent and Courtage 🟢
# =============================================================================
CLIENT_SCHEMA = StructType([
    StructField("NOCLT", StringType(), True),
    StructField("CDSIRET", StringType(), True),
    StructField("CDSIREN", StringType(), True),
    StructField("CDNAF", StringType(), True),
])


# =============================================================================
# RISK RATING - Euler Hermes
# =============================================================================
BINSEE_HISTO_NOTE_RISQUE_SCHEMA = StructType([
    StructField("CDSIREN", StringType(), True),
    StructField("CDNOTE", StringType(), True),
    StructField("DTDEB_VALID", DateType(), True),
    StructField("DTFIN_VALID", DateType(), True),
])


# =============================================================================
# SPECIAL PRODUCTS - IPFM0024 - Product Activities
# =============================================================================
IPFSPE_IPFM0024_SCHEMA = StructType([
    StructField("NOPOL", StringType(), True),
    StructField("NOINT", StringType(), True),
    StructField("CDPROD", StringType(), True),
    StructField("CDACTPRF01", StringType(), True),  # Principal activity
    StructField("CDACTPRF02", StringType(), True),  # Secondary activity
])


# =============================================================================
# SPECIAL PRODUCTS - IPFM63 - Professional Activities
# =============================================================================
IPFSPE_IPFM63_SCHEMA = StructType([
    StructField("NOPOL", StringType(), True),
    StructField("NOINT", StringType(), True),
    StructField("CDPROD", StringType(), True),
    StructField("ACTPRIN", StringType(), True),
    StructField("ACTSEC1", StringType(), True),
    StructField("CDNAF", StringType(), True),
    StructField("MTCA1", DoubleType(), True),
])


# =============================================================================
# SPECIAL PRODUCTS - IPFM99 - Generic Activities
# =============================================================================
IPFSPE_IPFM99_SCHEMA = StructType([
    StructField("NOPOL", StringType(), True),
    StructField("NOINT", StringType(), True),
    StructField("CDPROD", StringType(), True),
    StructField("CDACPR1", StringType(), True),
    StructField("CDACPR2", StringType(), True),
    StructField("MTCA", DoubleType(), True),
])


# =============================================================================
# CLIENT BASE W6
# =============================================================================
W6_BASECLI_INV_SCHEMA = StructType([
    StructField("NOCLT", StringType(), True),
    StructField("CDAPET", StringType(), True),  # NAF code APE
])


# =============================================================================
# MANAGEMENT POINTS REFERENCE - Two Different Tables 🟢
# Based on: SAS analysis PTF_MVTS_AZ_MACRO.sas L498 and REF_segmentation_azec.sas L142
# =============================================================================
# TABLE_PT_GEST - Versioned (PT_GEST.PTGST_YYYYMM)
# Used in: AZ processor - provides Upper_MID for segmentation
TABLE_PT_GEST_SCHEMA = StructType([
    StructField("PTGST", StringType(), True),
    StructField("UPPER_MID", StringType(), True),  # Management level classification
])

# PTGST_STATIC - Static reference (SAS_C.PTGST)
# Used in: AZEC enrichment - provides REGION and P_Num
PTGST_STATIC_SCHEMA = StructType([
    StructField("PTGST", StringType(), True),
    StructField("REGION", StringType(), True),  # Region classification
    StructField("P_NUM", StringType(), True),  # Management point number
])


# Deleted: CPRODUIT_CATALOG_SCHEMA (unused, identical to PRDCAP_SCHEMA)


# =============================================================================
# PRODUCT CAPITALS REFERENCE 🟢
# =============================================================================
PRDCAP_SCHEMA = StructType([
    StructField("CDPROD", StringType(), True),
    StructField("LBTPROD", StringType(), True),
])


# =============================================================================
# PRODUCT SEGMENTATION - Consolidated
# Used for monthly or reference segmentation (segmentprdt_{vision})
# =============================================================================
SEGMENTPRDT_SCHEMA = StructType([
    StructField("CPROD", StringType(), True),
    StructField("CDPOLE", StringType(), True),
    StructField("CMARCH", StringType(), True),
    StructField("CSEG", StringType(), True),
    StructField("CSSSEG", StringType(), True),
])


# =============================================================================
# MIGRATION REFERENCE - ref_mig_azec_vs_ims
# SAS: PTF_MVTS_AZEC_MACRO.sas L94-106 - Maps AZEC policies to IMS
# =============================================================================
REF_MIG_AZEC_VS_IMS_SCHEMA = StructType([
    StructField("NOPOL_AZEC", StringType(), True),  # AZEC policy number
    StructField("NOPOL_IMS", StringType(), True),  # Corresponding IMS policy
    StructField("DATE_MIG", DateType(), True),  # Migration date
])


# =============================================================================
# INDEXATION - Construction cost indices
# SAS: indexation_v2.sas - $INDICE format table for capital adjustment
# Note: SAS uses OPTIONS FMTSEARCH=(INDICES) + PUT($INDICE.) format lookup
#       Python must replicate this lookup logic
# =============================================================================
INDICES_SCHEMA = StructType([
    StructField("annee", StringType(), True),  # Year
    StructField("mois", StringType(), True),  # Month
    StructField("indice", DoubleType(), True),  # Index value
])


# =============================================================================
# ISIC CODIFICATION TABLES 🟢
# SAS: CODIFICATION_ISIC_CONSTRUCTION.sas - ISIC assignment hierarchy
# =============================================================================
# Activity-based ISIC mapping 🟢
MAPPING_ISIC_CONST_ACT_SCHEMA = StructType([
    StructField("ACTPRIN", StringType(), True),  # Principal activity code
    StructField("CDNAF08", StringType(), True),  # NAF 2008 code
    StructField("CDTRE", StringType(), True),  # Trade code
    StructField("CDNAF03", StringType(), True),  # NAF 2003 code
    StructField("CDISIC", StringType(), True),  # ISIC code
])

# Construction site destination to ISIC mapping 🟢
MAPPING_ISIC_CONST_CHT_SCHEMA = StructType([
    StructField("DESTI_ISIC", StringType(), True),  # Destination ISIC
    StructField("CDNAF08", StringType(), True),  # NAF 2008 code
    StructField("CDTRE", StringType(), True),  # Trade code
    StructField("CDNAF03", StringType(), True),  # NAF 2003 code
    StructField("CDISIC", StringType(), True),  # ISIC code
])

# NAF 2003 to ISIC mapping 🟢
MAPPING_CDNAF2003_ISIC_SCHEMA = StructType([
    StructField("CDNAF_2003", StringType(), True),  # NAF 2003 code
    StructField("ISIC_Code", StringType(), True),  # ISIC code
])

# NAF 2008 to ISIC mapping 🟢
MAPPING_CDNAF2008_ISIC_SCHEMA = StructType([
    StructField("CDNAF_2008", StringType(), True),  # NAF 2008 code
    StructField("ISIC_Code", StringType(), True),  # ISIC code
])

# ISIC hazard grades reference table 🟢
TABLE_ISIC_TRE_NAF_SCHEMA = StructType([
    StructField("ISIC_Code", StringType(), True),
    StructField("HAZARD_GRADES_FIRE", StringType(), True),
    StructField("HAZARD_GRADES_BI", StringType(), True),
    StructField("HAZARD_GRADES_RCA", StringType(), True),
    StructField("HAZARD_GRADES_RCE", StringType(), True),
    StructField("HAZARD_GRADES_TRC", StringType(), True),
    StructField("HAZARD_GRADES_RCD", StringType(), True),
    StructField("HAZARD_GRADES_DO", StringType(), True),
])

# IRD Suivi Engagements - NAF 2008 tracking 🟢
IRD_SUIVI_ENGAGEMENTS_SCHEMA = StructType([
    StructField("NOPOL", StringType(), True),
    StructField("CDPROD", StringType(), True),
    StructField("CDNAF08", StringType(), True),
    StructField("CDISIC", StringType(), True),
])


# =============================================================================
# ISIC LOCAL-TO-GLOBAL MAPPING 🟢
# SAS: ISIC.ISIC_LG_202306 - Maps local ISIC codes to global ISIC codes
# =============================================================================
ISIC_LG_SCHEMA = StructType([
    StructField("ISIC_Local", StringType(), True),  # Local ISIC code
    StructField("ISIC_Global", StringType(), True),  # Global ISIC code (ISIC_Code_GBL)
])


# =============================================================================
# DO_DEST - CONSTRUCTION DESTINATION REFERENCE
# SAS: DEST.DO_DEST202110 - Maps NOPOL to construction site destination
# =============================================================================
DO_DEST_SCHEMA = StructType([
    StructField("NOPOL", StringType(), True),  # Policy number
    StructField("DESTINAT", StringType(), True),  # Construction destination (Habitation, Autres)
])


# =============================================================================
# TABLE_SEGMENTATION_AZEC_MML - AZEC PRODUCT SEGMENTATION
# SAS: REF.TABLE_SEGMENTATION_AZEC_MML - Product-based segmentation for AZEC
# =============================================================================
TABLE_SEGMENTATION_AZEC_MML_SCHEMA = StructType([
    StructField("PRODUIT", StringType(), True),  # AZEC product code
    StructField("SEGMENT", StringType(), True),  # Segment classification
    StructField("CMARCH", StringType(), True),  # Market code
    StructField("CSEG", StringType(), True),  # Segment code
    StructField("CSSSEG", StringType(), True),  # Sub-segment code
    StructField("LMARCH", StringType(), True),  # Market label
    StructField("LSEG", StringType(), True),  # Segment label
    StructField("LSSSEG", StringType(), True),  # Sub-segment label
])


# =============================================================================
# TYPRD_2 - AZEC PRODUCT TYPE REFERENCE
# SAS: REF_segmentation_azec.sas L320-325 (SAS_C.TYPRD_2)
# Note: Used in AZEC segmentation merge with CONSTRCU for type_produit_2 enrichment
# =============================================================================
TYPRD_2_SCHEMA = StructType([
    StructField("CDPROD", StringType(), True),  # Product code
    StructField("POLICE", StringType(), True),  # Policy number (for join with CONSTRCU)
    StructField("SEGMENT", StringType(), True),  # Segment2 classification
    StructField("TYPE_PRODUIT", StringType(), True),  # Product type (type_produit_2)
])


# =============================================================================
# SCHEMA REGISTRY
# Maps file_group names from reading_config.json to schemas
# Only includes schemas for files currently available in datalake
# =============================================================================
SCHEMA_REGISTRY = {
    # Monthly Data Files (bronze/2025/09/)
    "rf_fr1_prm_dtl_midcorp_m": RF_FR1_PRM_DTL_MIDCORP_M_SCHEMA,
    "ipf_az": IPF_AZ_SCHEMA,
    "ipfm99_az": IPFM99_AZ_SCHEMA,
    "ird_risk_q45": IRD_RISK_COMMON_SCHEMA,  # Consolidated
    "ird_risk_q46": IRD_RISK_COMMON_SCHEMA,  # Consolidated
    "ird_risk_qan": IRD_RISK_QAN_SCHEMA,
    "ipfspe_ipfm0024": IPFSPE_IPFM0024_SCHEMA,
    "ipfm0024_1": IPFSPE_IPFM0024_SCHEMA,  # Reference data
    "ipfm0024_3": IPFSPE_IPFM0024_SCHEMA,  # Reference data
    "ipfspe_ipfm63": IPFSPE_IPFM63_SCHEMA,
    "ipfm63_1": IPFSPE_IPFM63_SCHEMA,  # Reference data
    "ipfm63_3": IPFSPE_IPFM63_SCHEMA,  # Reference data
    "ipfspe_ipfm99": IPFSPE_IPFM99_SCHEMA,
    "ipfm99_1": IPFSPE_IPFM99_SCHEMA,  # Reference data
    "ipfm99_3": IPFSPE_IPFM99_SCHEMA,  # Reference data
    
    # AZEC Reference Files (bronze/ref/)
    "polic_cu_azec": POLIC_CU_SCHEMA,  # FIXED: Now using POLIC_CU_SCHEMA which includes DATEXPIR
    "capitxcu_azec": CAPITXCU_SCHEMA,
    "incendcu_azec": INCENDCU_SCHEMA,
    "constrcu_azec": CONSTRCU_AZEC_SCHEMA,
    "rcentcu_azec": AZEC_FORMULE_SCHEMA,  # Consolidated
    "risteccu_azec": AZEC_FORMULE_SCHEMA,  # Consolidated
    "mulprocu_azec": MULPROCU_SCHEMA,
    "mpacu_azec": MPACU_SCHEMA,
    
    # Product & Segmentation Reference Files (bronze/ref/)
    "segmprdt_prdpfa1": SEGMPRDT_SCHEMA,
    "segmprdt_prdpfa3": SEGMPRDT_SCHEMA,
    "segmentprdt": SEGMENTPRDT_SCHEMA,
    "prdcap": PRDCAP_SCHEMA,
    "cproduit": CPRODUIT_SCHEMA,
    "lob": LOB_SCHEMA,
    "garantcu": GARANTCU_SCHEMA,
    "catmin": CATMIN_SCHEMA,
    
    # Management & Client Reference Files (bronze/ref/)
    "table_pt_gest": TABLE_PT_GEST_SCHEMA,  # Versioned - Upper_MID
    "ptgst_static": PTGST_STATIC_SCHEMA,  # Static - REGION, P_Num
    "client1": CLIENT_SCHEMA,
    "client3": CLIENT_SCHEMA,
    "w6_basecli_inv": W6_BASECLI_INV_SCHEMA,
    "binsee_histo_note_risque": BINSEE_HISTO_NOTE_RISQUE_SCHEMA,
    
    # New reference tables for remaining parity gaps
    "ref_mig_azec_vs_ims": REF_MIG_AZEC_VS_IMS_SCHEMA,
    "indices": INDICES_SCHEMA,
    "mapping_isic_const_act": MAPPING_ISIC_CONST_ACT_SCHEMA,
    "mapping_isic_const_cht": MAPPING_ISIC_CONST_CHT_SCHEMA,
    "mapping_cdnaf2003_isic": MAPPING_CDNAF2003_ISIC_SCHEMA,
    "mapping_cdnaf2008_isic": MAPPING_CDNAF2008_ISIC_SCHEMA,
    "table_isic_tre_naf": TABLE_ISIC_TRE_NAF_SCHEMA,
    "ird_suivi_engagements": IRD_SUIVI_ENGAGEMENTS_SCHEMA,
    
    # Additional schemas for 100% parity
    "isic_lg": ISIC_LG_SCHEMA,
    "do_dest": DO_DEST_SCHEMA,
    "table_segmentation_azec_mml": TABLE_SEGMENTATION_AZEC_MML_SCHEMA,
    "typrd_2": TYPRD_2_SCHEMA,
}


def get_schema(file_group: str) -> StructType:
    """
    Get schema for a file group.
    
    Args:
        file_group: File group name from reading_config.json
        
    Returns:
        PySpark StructType schema, or None if not found
        
    Note:
        Returns None for file groups without schemas (allows column discovery).
        This enables reading files with automatic type inference to explore 
        available columns before defining strict schemas.
        
    Example:
        >>> schema = get_schema("ipf_az")  # Returns IPF_AZ_SCHEMA
        >>> schema = get_schema("unknown")  # Returns None (not in registry)
    """
    if file_group not in SCHEMA_REGISTRY:
        return None
    return SCHEMA_REGISTRY[file_group]