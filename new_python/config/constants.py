"""
Business constants for Construction Data Pipeline.
All values in lowercase for consistency.
"""

# =========================================================================
# DIRECTION COMMERCIALE
# =========================================================================

class DIRCOM:
    """Direction commerciale codes."""
    AZ = "az"
    AZEC = "azec"


# =========================================================================
# POLES
# =========================================================================

class POLE:
    """Pole codes for AZ channel distinction."""
    AGENT = "1"      # PTF16
    COURTAGE = "3"   # PTF36

# =========================================================================
# GOLD LAYER OUTPUT SCHEMA - PTF_MVT PIPELINE
# =========================================================================

# PTF_MVT Gold layer output columns (exact SAS schema)
# Based on: PTF_MVTS_CONSOLIDATION_MACRO.sas L441-442 (RETAIN statement)
# NOTE: Future pipelines (EMISSIONS, CAPITAUX) will have their own column lists:
#       - GOLD_COLUMNS_EMISSIONS
#       - GOLD_COLUMNS_CAPITAUX
GOLD_COLUMNS_PTF_MVT = [
    # Primary keys (SAS L441)
    'nopol', 'nopolli1', 'dircom', 'cdpole', 'noint', 'cdprod',
    # Segmentation (SAS L441)
    'cmarch', 'cseg', 'cssseg',
    # Dates
    'dtcrepol', 'mois_echeance', 'jour_echeance',
    'dtresilp', 'dttraar', 'dteffan', 'dttraan',
    # Policy attributes
    'cdnatp', 'cdsitp', 'ptgst', 'cdreg', 'cdgecent', 'mtca', 'cdnaf', 'cdtre',
    # Coassurance
    'cdcoass', 'coass', 'top_coass',
    # Type
    'type_affaire',
    # Premiums
    'primes_ptf_intemp', 'primes_ptf_100_intemp', 'part_cie',
    'primes_ptf', 'primes_afn', 'primes_res', 'primes_rpt', 'primes_rpc',
    # Movement indicators
    'nbptf', 'nbafn', 'nbres', 'nbrpt', 'nbrpc',
    'nbafn_anticipe', 'nbres_anticipe',
    # Exposure
    'expo_ytd', 'expo_gli', 'dt_deb_expo', 'dt_fin_expo',
    # Flags
    'top_temp', 'top_revisable',
    # Termination details
    'cdmotres', 'cdcasres', 'cdpolrvi',
    # Vision
    'vision', 'exevue', 'moisvue',
    # Client
    'noclt', 'nmclt', 'cdsiret', 'cdsiren',
    # Risk address
    'cdfract', 'quarisq', 'nmrisq', 'nmsrisq', 'resrisq',
    'ruerisq', 'lidirisq', 'posrisq', 'vilrisq',
    'posacta_ri', 'rueacta_ri', 'cediacta_ri',
    # Segmentation extended
    'upper_mid', 'segment2', 'type_produit_2',
    # Construction site details (from IRD risk)
    'ctdeftra', 'dstcsc', 'dtouchan', 'dtrectrx', 'dtrcppr', 'dtreffin',
    'lbqltsou', 'actprin', 'ctprvtrv', 'mtsmpr', 'lbnattrv',
    # Revision
    'critere_revision', 'cdgrev',
    # Capital amounts (from IPFM99/IPFM63/IPFM0024)
    'mtcaenp', 'mtcasst', 'mtcavnt',
    # Activity codes (from special products)
    'typeact',
    # Destination
    'destinat',
    # NAF codes (from various sources)
    'cdnaf08_w6', 'cdnaf03_cli', 'cdnaf2008',
    # ISIC codes
    'isic_code', 'isic_code_gbl', 'isic_code_sui',
    'origine_isic', 'destinat_isic',
    # Hazard grades
    'hazard_grades_fire', 'hazard_grades_bi', 'hazard_grades_rca',
    'hazard_grades_rce', 'hazard_grades_trc', 'hazard_grades_rcd', 'hazard_grades_do',
    # Risk note
    'note_euler',
    # Special flags
    'top_berlioz', 'top_partenariat'
]


# =========================================================================
# GOLD LAYER OUTPUT SCHEMA - EMISSIONS PIPELINE
# =========================================================================

# Emissions Gold layer output columns - POL_GARP (by guarantee)
# Based on: EMISSIONS_RUN.sas L284-294
# 14 columns total
GOLD_COLUMNS_EMISSIONS_POL_GARP = [
    'vision', 'dircom', 'cdpole', 'nopol', 'cdprod', 'noint', 'cgarp',
    'cmarch', 'cseg', 'cssseg', 'cd_cat_min',
    'primes_x', 'primes_n', 'mtcom_x'
]

# Emissions Gold layer output columns - POL (aggregated by policy)
# Based on: EMISSIONS_RUN.sas L298-307
# 12 columns total
GOLD_COLUMNS_EMISSIONS_POL = [
    'vision', 'dircom', 'nopol', 'noint', 'cdpole', 'cdprod',
    'cmarch', 'cseg', 'cssseg',
    'primes_x', 'primes_n', 'mtcom_x'
]



# =========================================================================
# MARKET & SEGMENT CODES
# =========================================================================
# 
# IMPORTANT: File naming convention vs Data filtering
# -----------------------------------------------------
# 
# FILE NAMING (Bronze layer):
#   - IPFE16 = Agent channel (1) + Construction market (6)
#   - IPFE36 = Courtier channel (3) + Construction market (6)
#   - First digit: Distribution channel (1=Agent, 3=Courtier)
#   - Second digit: Market/Product line (6=Construction, 7=Healthcare, etc.)
# 
# Example for different markets:
#   - Construction: IPFE16_*.csv.gz, IPFE36_*.csv.gz
#   - Healthcare:   IPFE17_*.csv.gz, IPFE37_*.csv.gz  (if existed)
# 
# DATA FILTERING (this constant):
#   - The values below filter the DATA by the 'cmarch' and 'csegt' columns
#   - These are applied AFTER reading the files
#   - The filter matches the market code in the file naming
#   - cmarch="6" means we only process Construction market records
# 
# TO CHANGE MARKETS:
#   1. Update file patterns in reading_config.json (e.g., IPFE17/IPFE37)
#   2. Update these constants to match the new market code
#   3. No code changes needed - just configuration!
# =========================================================================     

class MARKET_CODE:
    """Market codes."""
    MARKET = "6"  # market code (matches IPFE1**6**, IPFE3**6**)
    SEGMENT = "2"  # segment code

# =========================================================================
# POLICY STATUS CODES
# =========================================================================

class POLICY_STATUS:
    """Policy status codes (cdsitp)."""
    EXCLUDED = ["4", "5"]  # Status to exclude


# =========================================================================
# POLICY NATURE CODES
# =========================================================================

class POLICY_NATURE:
    """Policy nature codes (cdnatp)."""
    VALID = ["R", "O", "T", "C"]  # Valid nature codes
    TEMPORARY = "T"                # Temporary contracts


# =========================================================================
# RISK INDICATOR
# =========================================================================

class RISK_INDICATOR:
    """Risk indicator (cdri)."""
    EXCLUDED = "X"


# =========================================================================
# COASSURANCE TYPES
# =========================================================================

class COASSURANCE_TYPE:
    """Coassurance type codes."""
    APERITION_CODES = ["3", "6"]
    ACCEPTEE_CODES = ["4", "5"]
    INTERNATIONALE_CODE = "8"
    
    APERITION_LABEL = "APERITION"
    ACCEPTEE_LABEL = "COASS. ACCEPTEE"
    INTERNATIONALE_LABEL = "ACCEPTATION INTERNATIONALE"
    AUTRES_LABEL = "AUTRES"
    SANS_LABEL = "SANS COASSURANCE"
    COASSURANCE_LABEL = "COASSURANCE"


# =========================================================================
# LONG-TERM CONTRACT TYPES
# =========================================================================

LTA_TYPES = ["QAW", "QBJ", "QBK", "QBB", "QBM"]  # Long-term contract types (tydrisi) - Fixed: QAM→QAW per SAS L243

# =========================================================================
# GOLD LAYER OUTPUT SCHEMA - CAPITAUX PIPELINE
# =========================================================================

# Capitaux Gold layer output columns - AZ_AZEC_CAPITAUX
# Based on: CAPITAUX_CONSOLIDATION_MACRO.sas L17-68
# 20 columns total
GOLD_COLUMNS_CAPITAUX = [
    # Identifiers
    'dircom', 'nopol', 'cdpole', 'cdprod',
    # Segmentation
    'cmarch', 'cseg', 'cssseg',
    # Indexed capitals (WITH indexation)
    'value_insured_100_ind', 'perte_exp_100_ind', 'risque_direct_100_ind',
    'limite_rc_100_par_sin', 'limite_rc_100_par_an', 'limite_rc_100',
    'smp_100_ind', 'lci_100_ind',
    # Non-indexed capitals (WITHOUT indexation)
    # Note: AZEC has these set to NULL
    'value_insured_100', 'perte_exp_100', 'risque_direct_100',
    'smp_100', 'lci_100'
]

