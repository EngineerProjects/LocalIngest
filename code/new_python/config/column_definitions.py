"""
Définitions des colonnes pour toutes les couches du pipeline Construction.

Organisation:
- BRONZE : Schémas de lecture (format DDL PySpark) 
- GOLD : Colonnes de sortie (listes Python)

Conventions:
- Colonnes en minuscules (lowercase)
- 4-6 colonnes par ligne pour lisibilité
- Types : STRING, INT, DOUBLE, DATE, TIMESTAMP
"""

# =============================================================================
# COUCHE BRONZE - Schémas de lecture (DDL)
# =============================================================================

# Données portefeuille - Canaux Agent (IPF16) et Courtage (IPF36)
# Sources: ipf16.csv.gz, ipf36.csv.gz | 126 colonnes
IPF_SCHEMA = """
cdprod STRING, nopol STRING, noclt STRING, nmclt STRING, noint STRING, nmacta STRING,
posacta STRING, rueacta STRING, cediacta STRING,
dtcrepol DATE, dteffan DATE, dttraan DATE, dtresilp DATE, dttraar DATE, dttypli1 DATE,
dttypli2 DATE, dttypli3 DATE, dtechann INT, dtouchan DATE, dtrcppr DATE, dtrectrx DATE,
dtrcpre DATE, dtefsitt DATE,
cdnatp STRING, txcede DOUBLE, ptgst STRING, cmarch STRING, cdsitp STRING, csegt STRING,
cssegt STRING, cdri STRING, cdtypli1 STRING, cdtypli2 STRING, cdtypli3 STRING,
mtprprto DOUBLE, prcdcie DOUBLE, mtcaf DOUBLE, fncmaca DOUBLE, mtsmpr DOUBLE,
cdpolqpl STRING, cdtpcoa STRING, cdcieori STRING, cdpolrvi STRING, cdgrev STRING,
cdsitmgr STRING, cdgecent STRING, cdmotres STRING, nopolli1 STRING, cdcasres STRING,
mtcapi1 DOUBLE, mtcapi2 DOUBLE, mtcapi3 DOUBLE, mtcapi4 DOUBLE, mtcapi5 DOUBLE, mtcapi6 DOUBLE,
mtcapi7 DOUBLE, mtcapi8 DOUBLE, mtcapi9 DOUBLE, mtcapi10 DOUBLE, mtcapi11 DOUBLE, mtcapi12 DOUBLE,
mtcapi13 DOUBLE, mtcapi14 DOUBLE,
lbcapi1 STRING, lbcapi2 STRING, lbcapi3 STRING, lbcapi4 STRING, lbcapi5 STRING, lbcapi6 STRING,
lbcapi7 STRING, lbcapi8 STRING, lbcapi9 STRING, lbcapi10 STRING, lbcapi11 STRING, lbcapi12 STRING,
lbcapi13 STRING, lbcapi14 STRING,
cdprvb1 STRING, cdprvb2 STRING, cdprvb3 STRING, cdprvb4 STRING, cdprvb5 STRING, cdprvb6 STRING,
cdprvb7 STRING, cdprvb8 STRING, cdprvb9 STRING, cdprvb10 STRING, cdprvb11 STRING, cdprvb12 STRING,
cdprvb13 STRING, cdprvb14 STRING,
prprvc1 DOUBLE, prprvc2 DOUBLE, prprvc3 DOUBLE, prprvc4 DOUBLE, prprvc5 DOUBLE, prprvc6 DOUBLE,
prprvc7 DOUBLE, prprvc8 DOUBLE, prprvc9 DOUBLE, prprvc10 DOUBLE, prprvc11 DOUBLE, prprvc12 DOUBLE,
prprvc13 DOUBLE, prprvc14 DOUBLE,
cdfract STRING, quarisq STRING, nmrisq STRING, nmsrisq STRING, resrisq STRING, ruerisq STRING,
lidirisq STRING, posrisq STRING, vilrisq STRING, cdreg STRING,
cdnaf STRING, cdtre STRING, cdactpro STRING, actprin STRING, tydris1 STRING, opapoffr STRING,
ctdeftra STRING, ctprvtrv STRING, lbnattrv STRING, dstcsc STRING, lbqltsou STRING
"""

# Données IPFM99 (Agent et Courtage - fichiers complémentaires)
# Sources: 3SPEIPFM99_IPF_*.csv.gz, E1SPEIPFM99_IPF_*.csv.gz | 9 colonnes
IPFM99_SCHEMA = """
cdprod STRING, nopol STRING, noint STRING, cdacpr1 STRING, cdacpr2 STRING,
mtca DOUBLE, mtcaenp DOUBLE, mtcasst DOUBLE, mtcavnt DOUBLE
"""

# Données risques IRD Q45/Q46 (schéma commun)
# Sources: ird_risk_q45_*.csv, ird_risk_q46_*.csv | 8 colonnes
IRD_RISK_COMMON_SCHEMA = """
nopol STRING, dtouchan DATE, dtrectrx DATE, dtreffin DATE,
ctprvtrv STRING, ctdeftra STRING, lbnattrv STRING, lbdstcsc STRING
"""

# Données risques IRD QAN
# Source: ird_risk_qan_*.csv | 5 colonnes
IRD_RISK_QAN_SCHEMA = """
nopol STRING, dtouchan DATE, ctprvtrv STRING, ctdeftra STRING, lbnattrv STRING
"""

# Référence INCENDCU (données incendie)
# Source: incendcu.csv | 6 colonnes
INCENDCU_SCHEMA = """
police STRING, produit STRING, cod_naf STRING, cod_tre STRING,
mt_baspe DOUBLE, mt_basdi DOUBLE
"""

# Référence CONSTRCU (données construction)
# Source: constrcu.csv | 13 colonnes
CONSTRCU_SCHEMA = """
datfinch DATE, datouvch DATE, datrecep DATE, dest_loc STRING, formule STRING,
ldestloc STRING, lqualite STRING, ltypmar1 STRING, mnt_glob DOUBLE, nat_cnt STRING,
police STRING, produit STRING, typmarc1 STRING
"""

# Référence segmentation produits (PRDPFA1/PRDPFA3)
# Sources: prdpfa1.csv, prdpfa3.csv | 8 colonnes
SEGMPRDT_SCHEMA = """
cmarch STRING, cprod STRING, cseg STRING, cssseg STRING,
lmarch STRING, lprod STRING, lseg STRING, lssseg STRING
"""

# Référence LOB (Line of Business)
# Source: lob.csv | 14 colonnes
LOB_SCHEMA = """
produit STRING, cdprod STRING, cprod STRING, cmarch STRING, lmarch STRING, lmarch2 STRING,
cseg STRING, lseg STRING, lseg2 STRING, cssseg STRING, lssseg STRING, lssseg2 STRING,
lprod STRING, segment STRING
"""

# Référence produits CPRODUIT
# Source: cproduit.csv | 4 colonnes (casse mixte dans source)
CPRODUIT_SCHEMA = """
cprod STRING, type_produit_2 STRING, segment STRING, segment_3 STRING
"""

# Référence garanties
# Source: garantcu.csv | 3 colonnes
GARANTCU_SCHEMA = """
police STRING, garantie STRING, branche STRING
"""

# Référence catégories minimales
# Source: import_catmin.csv | 3 colonnes
CATMIN_SCHEMA = """
produit STRING, garantie STRING, catmin5 STRING
"""

# Données polices (master data)
# Source: POLIC_CU.csv | 20 colonnes
POLIC_CU_SCHEMA = """
codecoas STRING, cpcua DOUBLE, datafn DATE, datexpir DATE, datfin DATE, datresil DATE,
datterme DATE, duree STRING, echeanjj INT, echeanmm INT, effetpol DATE, etatpol STRING,
finpol DATE, gestsit STRING, indregul STRING, intermed STRING, motifres STRING,
nomcli STRING, origres STRING, partbrut DOUBLE, poingest STRING, police STRING,
prime DOUBLE, produit STRING, rmplcant STRING, typcontr STRING
"""

# Données capitaux
# Source: CAPITXCU.csv | 6 colonnes
CAPITXCU_SCHEMA = """
police STRING, produit STRING, smp_sre STRING, brch_rea STRING,
capx_100 DOUBLE, capx_cua DOUBLE
"""

# Données formules (RCENTCU + RISTECCU)
# Sources: rcentcu.csv, risteccu.csv | 7 colonnes
FORMULE_SCHEMA = """
police STRING, produit STRING, cod_naf STRING, formule STRING,
formule2 STRING, formule3 STRING, formule4 STRING
"""

# Données multirisque professionnel
# Source: mulprocu.csv | 2 colonnes
MULPROCU_SCHEMA = """
police STRING, chiffaff DOUBLE
"""

# Données MPA
# Source: mpacu.csv | 2 colonnes
MPACU_SCHEMA = """
police STRING, cod_naf STRING
"""

# Référence clients
# Source: client.csv | 4 colonnes
CLIENT_SCHEMA = """
noclt STRING, cdsiret STRING, cdsiren STRING, cdnaf STRING
"""

# Référence notation risque Euler Hermes
# Source: binsee_histo_note_risque.csv | 4 colonnes
BINSEE_HISTO_NOTE_RISQUE_SCHEMA = """
cdsiren STRING, cdnote STRING, dtdeb_valid DATE, dtfin_valid DATE
"""

# Référence activités produits spéciaux IPFM0024
# Source: ipfspe_ipfm0024.csv | 5 colonnes
IPFSPE_IPFM0024_SCHEMA = """
nopol STRING, noint STRING, cdprod STRING, cdactprf01 STRING, cdactprf02 STRING
"""

# Référence activités professionnelles IPFM63
# Source: ipfspe_ipfm63.csv | 7 colonnes
IPFSPE_IPFM63_SCHEMA = """
nopol STRING, noint STRING, cdprod STRING, actprin STRING,
actsec1 STRING, cdnaf STRING, mtca1 DOUBLE
"""

# Référence activités génériques IPFM99
# Source: ipfspe_ipfm99.csv | 6 colonnes
IPFSPE_IPFM99_SCHEMA = """
nopol STRING, noint STRING, cdprod STRING, cdacpr1 STRING, cdacpr2 STRING, mtca DOUBLE
"""

# Référence base clients W6
# Source: w6_basecli_inv.csv | 2 colonnes
W6_BASECLI_INV_SCHEMA = """
noclt STRING, cdapet STRING
"""

# Référence points de gestion (versionné)
# Source: pt_gest.ptgst_YYYYMM | 2 colonnes
TABLE_PT_GEST_SCHEMA = """
ptgst STRING, upper_mid STRING
"""

# Référence points de gestion (statique)
# Source: sas_c.ptgst | 3 colonnes
PTGST_STATIC_SCHEMA = """
ptgst STRING, region STRING, p_num STRING
"""

# Référence capitaux produits
# Source: prdcap.csv | 2 colonnes
PRDCAP_SCHEMA = """
cdprod STRING, lbtprod STRING
"""

# Référence segmentation produits consolidée
# Source: segmentprdt_{vision} | 5 colonnes
SEGMENTPRDT_SCHEMA = """
cprod STRING, cdpole STRING, cmarch STRING, cseg STRING, cssseg STRING
"""

# Référence migration AZEC vers IMS
# Source: ref_mig_azec_vs_ims.csv | 2 colonnes
REF_MIG_AZEC_VS_IMS_SCHEMA = """
nopol_azec STRING, nopol_ims STRING
"""

# Référence indices de construction
# Source: indices.csv | 3 colonnes
INDICES_SCHEMA = """
annee STRING, mois STRING, indice DOUBLE
"""

# Référence mapping ISIC - Activités
# Source: mapping_isic_const_act.csv | 5 colonnes
MAPPING_ISIC_CONST_ACT_SCHEMA = """
actprin STRING, cdnaf08 STRING, cdtre STRING, cdnaf03 STRING, cdisic STRING
"""

# Référence mapping ISIC - Chantiers
# Source: mapping_isic_const_cht.csv | 5 colonnes
MAPPING_ISIC_CONST_CHT_SCHEMA = """
desti_isic STRING, cdnaf08 STRING, cdtre STRING, cdnaf03 STRING, cdisic STRING
"""

# Référence mapping NAF 2003 vers ISIC
# Source: mapping_cdnaf2003_isic.csv | 2 colonnes
MAPPING_CDNAF2003_ISIC_SCHEMA = """
cdnaf_2003 STRING, isic_code STRING
"""

# Référence mapping NAF 2008 vers ISIC
# Source: mapping_cdnaf2008_isic.csv | 2 colonnes
MAPPING_CDNAF2008_ISIC_SCHEMA = """
cdnaf_2008 STRING, isic_code STRING
"""

# Référence ISIC - Grades de danger
# Source: table_isic_tre_naf.csv | 8 colonnes
TABLE_ISIC_TRE_NAF_SCHEMA = """
isic_code STRING, hazard_grades_fire STRING, hazard_grades_bi STRING, hazard_grades_rca STRING,
hazard_grades_rce STRING, hazard_grades_trc STRING, hazard_grades_rcd STRING, hazard_grades_do STRING
"""

# Référence IRD suivi engagements
# Source: ird_suivi_engagements.csv | 4 colonnes
IRD_SUIVI_ENGAGEMENTS_SCHEMA = """
nopol STRING, cdprod STRING, cdnaf08 STRING, cdisic STRING
"""

# Référence ISIC Local vers Global
# Source: isic.isic_lg_202306 | 2 colonnes
ISIC_LG_SCHEMA = """
isic_local STRING, isic_global STRING
"""

# Référence destinations construction
# Source: do_dest.csv | 2 colonnes
DO_DEST_SCHEMA = """
dest_loc STRING, desti_isic STRING
"""

# Référence segmentation AZEC MML
# Source: table_segmentation_azec_mml.csv | 4 colonnes
TABLE_SEGMENTATION_AZEC_MML_SCHEMA = """
cdprod STRING, segment STRING, segment2 STRING, type_produit_2 STRING
"""

# Référence émissions ONE BI
# Source: rf_fr1_prm_dtl_midcorp_m_*.csv | 15 colonnes
RF_FR1_PRM_DTL_MIDCORP_M_SCHEMA = """
cd_niv_2_stc STRING, cd_int_stc STRING, nu_cnt_prm STRING, cd_prd_prm STRING, cd_statu_cts STRING,
dt_cpta_cts INT, dt_emis_cts DATE, dt_annu_cts DATE, mt_ht_cts DOUBLE, mt_cms_cts DOUBLE,
cd_cat_min STRING, cd_gar_princ STRING, cd_gar_prospctiv STRING, nu_ex_ratt_cts STRING, cd_marche STRING
"""

# =============================================================================
# COUCHE GOLD - Colonnes de sortie
# =============================================================================

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

# Legacy aliases for backwards compatibility
PTF_MVT_OUTPUT_COLUMNS = GOLD_COLUMNS_PTF_MVT
CAPITAUX_OUTPUT_COLUMNS = GOLD_COLUMNS_CAPITAUX
EMISSIONS_OUTPUT_COLUMNS = GOLD_COLUMNS_EMISSIONS_POL

# =============================================================================
# Registre des schémas (pour mapping automatique)
# =============================================================================

SCHEMA_REGISTRY = {
    'ipf': IPF_SCHEMA,
    'ipfm99': IPFM99_SCHEMA,
    'ird_risk_q45': IRD_RISK_COMMON_SCHEMA,
    'ird_risk_q46': IRD_RISK_COMMON_SCHEMA,
    'ird_risk_qan': IRD_RISK_QAN_SCHEMA,
    'incendcu': INCENDCU_SCHEMA,
    'constrcu': CONSTRCU_SCHEMA,
    'prdpfa1': SEGMPRDT_SCHEMA,
    'prdpfa3': SEGMPRDT_SCHEMA,
    'lob': LOB_SCHEMA,
    'cproduit': CPRODUIT_SCHEMA,
    'garantcu': GARANTCU_SCHEMA,
    'import_catmin': CATMIN_SCHEMA,
    'polic_cu': POLIC_CU_SCHEMA,
    'capitxcu': CAPITXCU_SCHEMA,
    'rcentcu': FORMULE_SCHEMA,
    'risteccu': FORMULE_SCHEMA,
    'mulprocu': MULPROCU_SCHEMA,
    'mpacu': MPACU_SCHEMA,
    'client': CLIENT_SCHEMA,
    'binsee_histo_note_risque': BINSEE_HISTO_NOTE_RISQUE_SCHEMA,
    'ipfspe_ipfm0024': IPFSPE_IPFM0024_SCHEMA,
    'ipfspe_ipfm63': IPFSPE_IPFM63_SCHEMA,
    'ipfspe_ipfm99': IPFSPE_IPFM99_SCHEMA,
    'w6_basecli_inv': W6_BASECLI_INV_SCHEMA,
    'table_pt_gest': TABLE_PT_GEST_SCHEMA,
    'ptgst_static': PTGST_STATIC_SCHEMA,
    'prdcap': PRDCAP_SCHEMA,
    'segmentprdt': SEGMENTPRDT_SCHEMA,
    'ref_mig_azec_vs_ims': REF_MIG_AZEC_VS_IMS_SCHEMA,
    'indices': INDICES_SCHEMA,
    'mapping_isic_const_act': MAPPING_ISIC_CONST_ACT_SCHEMA,
    'mapping_isic_const_cht': MAPPING_ISIC_CONST_CHT_SCHEMA,
    'mapping_cdnaf2003_isic': MAPPING_CDNAF2003_ISIC_SCHEMA,
    'mapping_cdnaf2008_isic': MAPPING_CDNAF2008_ISIC_SCHEMA,
    'table_isic_tre_naf': TABLE_ISIC_TRE_NAF_SCHEMA,
    'ird_suivi_engagements': IRD_SUIVI_ENGAGEMENTS_SCHEMA,
    'isic_lg': ISIC_LG_SCHEMA,
    'do_dest': DO_DEST_SCHEMA,
    'table_segmentation_azec_mml': TABLE_SEGMENTATION_AZEC_MML_SCHEMA,
    'rf_fr1_prm_dtl_midcorp_m': RF_FR1_PRM_DTL_MIDCORP_M_SCHEMA,
}


def get_schema(file_group: str) -> str:
    """
    Récupère le schéma DDL pour un groupe de fichiers.
    
    Args:
        file_group: Nom du groupe depuis reading_config.json
        
    Returns:
        String DDL du schéma, ou None si non trouvé
    """
    return SCHEMA_REGISTRY.get(file_group)
