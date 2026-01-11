"""
Constants for Construction Data Pipeline.

This module defines business constants used across processors:
- Direction Commerciale (DIRCOM) codes
- Pole (distribution channel) codes  
- Market codes (MARKET_CODE)
- LTA (Ligne de Travaux Assurés) types
- Gold layer column schemas

Based on SAS business rules and output schemas.
"""

# ============================================================================
# Direction Commerciale (DIRCOM)
# ============================================================================

class DIRCOM:
    """Direction Commerciale codes - SAS: DIRCOM column values"""
    AZ = "AZ"        # AZ channel (Agent + Courtage)
    AZEC = "AZEC"    # AZEC channel (Construction courtage)


# ============================================================================
# Pole (Distribution Channel)
# ============================================================================

class POLE:
    """
    Distribution channel codes - SAS: CDPOLE column values.
    
    SAS mapping (PTF_MVTS_AZ_MACRO.sas L69-76):
    - '1' = Agents (DCAG, DCPS, DIGITAL)
    - '3' = Courtage (BROKDIV, BROKMID)
    """
    AGENT = "  1"       # Agents (with spaces for SAS compatibility)
    COURTAGE = "3"      # Courtage


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
# LONG-TERM CONTRACT TYPES
# =========================================================================

LTA_TYPES = ["QAW", "QBJ", "QBK", "QBB", "QBM"]  # Long-term contract types (tydrisi) - Fixed: QAM→QAW per SAS L243



# ============================================================================
# Gold Layer Column Schemas
# ============================================================================

# PTF_MVT Gold Columns (AZ + AZEC + Consolidation)
# Based on actual SAS output: CUBE.MVT_PTF&vision. (110 columns)
GOLD_COLUMNS_PTF_MVT = [
    # Core Identifiers (7)
    'nopol', 'dircom', 'noint', 'cdpole', 'cdprod', 'cmarch', 'cseg',
    'cssseg', 'nopolli1', 'dtcrepol',
    
    # Risk Location (6)
    'posacta_ri', 'rueacta_ri', 'cediacta_ri',
    
    # Dates - Echeance (2)
    'mois_echeance', 'jour_echeance',
    
    # Dates - Additional (2)
    'dtresilp', 'dttraar',
    
    # Policy Attributes (7)
    'cdnatp', 'cdsitp', 'ptgst', 'cdreg', 'cdgecent',
    
    # Business Data (5)
    'mtca', 'cdnaf', 'cdtre', 'cdcoass', 'coass',
    
    # Flags (2)
    'top_coass', 'type_affaire',
    
    # Premiums - Portfolio (4)
    'primes_ptf_intemp', 'primes_ptf_100_intemp', 'part_cie', 'primes_ptf',
    
    # Movement Indicators (2)
    'nbptf', 'expo_ytd',
    
    # Exposures (3)
    'expo_gli', 'top_temp',
    
    # Termination (3)
    'cdmotres', 'cdcasres', 'cdpolrvi',
    
    # Vision (4)
    'vision', 'exevue', 'moisvue',
    
    # Client Info (10)
    'noclt', 'nmclt', 'cdfract', 'quarisq', 'nmrisq',
    'nmsrisq', 'resrisq', 'ruerisq', 'lidirisq', 'posrisq',
    'vilrisq',
    
    # Movements AFN/RES (6)
    'nbafn', 'nbres', 'nbafn_anticipe', 'nbres_anticipe',
    'primes_afn', 'primes_res',
    
    # Segment & Product Type (3)
    'upper_mid', 'dt_deb_expo', 'dt_fin_expo',
    'segment2', 'type_produit_2',
    
    # IRD Risk Data (12)
    'ctdeftra', 'dstcsc', 'dtouchan', 'dtrectrx',
    'dtrcppr', 'lbqltsou', 'dteffan', 'dttraan',
    'actprin', 'ctprvtrv', 'mtsmpr', 'lbnattrv',
    
    # Revision (5)
    'top_revisable', 'critere_revision', 'cdgrev',
    
    # Additional Movements (4)
    'nbrpt', 'nbrpc', 'primes_rpc', 'primes_rpt',
    
    # Additional Capitals (3)
    'mtcaenp', 'mtcasst', 'mtcavnt',
    
    # IRD Additional (1)
    'dtreffin',
    
    # Client Enrichment (3)
    'cdsiret', 'cdsiren', 'note_euler',
    
    # Destination (1)
    'destinat',
    
    # Activity Type (1)
    'typeact',
    
    # NAF Codes (4)
    'cdnaf08_w6', 'cdnaf03_cli', 'cdnaf2008',
    
    # ISIC Codes (9)
    'isic_code_sui', 'destinat_isic', 'isic_code', 'origine_isic',
    'hazard_grades_fire', 'hazard_grades_bi', 'hazard_grades_rca',
    'hazard_grades_rce', 'hazard_grades_trc', 'hazard_grades_rcd',
    'hazard_grades_do', 'isic_code_gbl',
    
    # Partnership Flags (2)
    'top_berlioz', 'top_partenariat'
]


# Emissions Gold Columns - POL_GARP (by guarantee)
# Based on SAS CUBE.PRIMES_EMISES&vision._POL_GARP
GOLD_COLUMNS_EMISSIONS_POL_GARP = [
    'vision', 'dircom', 'cdpole', 'nopol', 'cdprod', 'noint', 'cgarp',
    'cmarch', 'cseg', 'cssseg', 'cd_cat_min',
    'primes_x', 'primes_n', 'mtcom_x'
]


# Emissions Gold Columns - POL (aggregated by policy)
# Based on SAS CUBE.PRIMES_EMISES&vision._POL
GOLD_COLUMNS_EMISSIONS_POL = [
    'vision', 'dircom', 'nopol', 'noint', 'cdpole', 'cdprod',
    'cmarch', 'cseg', 'cssseg',
    'primes_x', 'primes_n', 'mtcom_x'
]


# Capitaux Gold Columns (AZ + AZEC Consolidated)
# Based on SAS CUBE.AZ_AZEC_CAPITAUX_&vision.
GOLD_COLUMNS_CAPITAUX = [
    # Identifiers
    'dircom', 'nopol', 'cdpole', 'cdprod',
    
    # Segmentation
    'cmarch', 'cseg', 'cssseg',
    
    # Capitals WITH indexation (_IND suffix)
    'value_insured_100_ind',    # Valeur assurée (PE + RD) indexed
    'perte_exp_100_ind',         # Perte d'exploitation indexed
    'risque_direct_100_ind',     # Risque direct indexed
    'smp_100_ind',               # SMP (Sinistre Maximum Possible) indexed
    'lci_100_ind',               # LCI (Limite Contractuelle Indemnité) indexed
    'limite_rc_100_par_sin',     # RC par sinistre
    'limite_rc_100_par_an',      # RC par an
    'limite_rc_100',             # RC max
    
    # Capitals WITHOUT indexation (only for AZ, NULL for AZEC)
    'value_insured_100',         # Valeur assurée (PE + RD) non-indexed
    'perte_exp_100',             # Perte d'exploitation non-indexed
    'risque_direct_100',         # Risque direct non-indexed
    'smp_100',                   # SMP non-indexed
    'lci_100'                    # LCI non-indexed
]
