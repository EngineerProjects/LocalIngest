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
# Based on SAS CUBE.MVT_PTF&vision. output
GOLD_COLUMNS_PTF_MVT = [
    # Identifiers
    'vision', 'dircom', 'cdpole', 'nopol', 'nopol11b', 'cdprod', 'noint',
    
    # Segmentation
    'cmarch', 'cseg', 'cssseg', 'segment', 'segment2', 'segment3',
    
    # Dates
    'dtcrepol', 'dteffan', 'dtresilp', 'dtechann', 'dtreslip',
    'mois_echeance', 'jour_echeance',
    
    # Business attributes
    'cdnatp', 'type_affaire', 'cdcasres', 'cdmotres', 'cdcoass',
    'prcdcie', 'part_cie', 'ptgst', 'upper_mid',
    
    # Movements
    'nbafn', 'nbres', 'nbrpt', 'nbrpc', 'nbptf',
    
    # Premiums
    'primes_afn', 'primes_res', 'primes_bpt', 'primes_bpc',
    'primes_ptf', 'primes_ptf_intemp', 'primes_ptf_100_intemp',
    
    # Exposures
    'expo_ytd', 'expo_gli',
    'dt_deb_expo', 'dt_fin_expo',
    
    # Capitals
    'mtcaenp', 'mtcasst', 'mtcavnt',
    
    # Flags
    'top_temp', 'top_suspension'
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
