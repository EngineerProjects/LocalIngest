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
# EXCLUSIONS
# =========================================================================

# Excluded intermediaries (NOINT) - from SAS PTF_MVTS_AZ_MACRO.sas L42-44
EXCLUDED_NOINT = [
    "H90061", "482001", "489090", "102030", "H90036",
    "H90059", "H99045", "H99059", "5B2000", "446000",
    "5R0001", "446118", "4F1004", "4A1400", "4A1500",
    "4A1600", "4A1700", "4A1800", "4A1900", "4L1010"
]

# Excluded AZEC intermediaries (SAS L37: codes courtiers fictifs)
EXCLUDED_AZEC_INTERMED = ["24050", "40490"]

# Excluded AZEC policies (SAS L37: police fictive)
EXCLUDED_AZEC_POLICE = ["012684940"]


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
