"""
Opérations de transformation spécifiques aux Capitaux.

Assistants simples pour :
- Extraction étendue des capitaux (7 types vs 4 dans business_logic.py)
- Normalisation sur une base 100%
- Règles métier Capitaux (complétude SMP, limites RC)
- Traitement des capitaux AZEC
"""

from pyspark.sql import DataFrame # type: ignore
from pyspark.sql.functions import col, when, lit, coalesce, greatest, sum as _sum # type: ignore
from typing import Dict, List


def extract_capitals_extended(
    df: DataFrame,
    config: Dict[str, Dict],
    num_capitals: int = 14,
    indexed: bool = False
) -> DataFrame:
    """
    Extrait les capitaux en faisant correspondre des mots-clés dans les colonnes de libellés.

    Version étendue supportant TOUS les types de capitaux pour le composant Capitaux :
    - SMP_100, LCI_100, PERTE_EXP_100, RISQUE_DIRECT_100 (depuis PTF_MVT)
    - LIMITE_RC_100_PAR_SIN, LIMITE_RC_100_PAR_AN (nouveau pour Capitaux)
    - SMP_PE_100, SMP_RD_100 (nouveau pour Capitaux)

    Paramètres :
        df : DataFrame en entrée
        config : Configuration d'extraction des capitaux depuis JSON
        num_capitals : Nombre de colonnes de capital à vérifier (défaut 14)
        indexed : Si True, utilise les colonnes indexées (mtcapi1i), sinon non indexées (mtcapi1)

    Retourne :
        DataFrame avec les colonnes de capital ajoutées
    """
    suffix = 'i' if indexed else ''
    mtcapi_prefix = f'mtcapi'
    lbcapi_prefix = 'lbcapi'

    # Pour chaque type de capital dans la config
    for capital_name, capital_config in config.items():
        # Ignorer les clés de commentaire et métadonnées
        if capital_name.startswith('_'):
            continue

        keywords = capital_config.get('keywords', [])
        exclude_keywords = capital_config.get('exclude_keywords', [])

        # Initialisation à 0
        capital_expr = lit(0.0)

        # Boucle sur mtcapi1-14 / lbcapi1-14
        for i in range(1, num_capitals + 1):
            mtcapi_col = f'{mtcapi_prefix}{i}{suffix}'
            lbcapi_col = f'{lbcapi_prefix}{i}'

            if mtcapi_col not in df.columns or lbcapi_col not in df.columns:
                continue

            # Construire la condition de correspondance
            match_condition = lit(False)
            for keyword in keywords:
                match_condition = match_condition | col(lbcapi_col).contains(keyword)

            # Construire la condition d'exclusion
            if exclude_keywords:
                for exclude_kw in exclude_keywords:
                    match_condition = match_condition & ~col(lbcapi_col).contains(exclude_kw)

            # Prendre le MAX : si le libellé correspond, utiliser ce montant de capital
            capital_expr = greatest(
                capital_expr,
                when(match_condition, coalesce(col(mtcapi_col), lit(0.0))).otherwise(lit(0.0))
            )

        # Ajouter le suffixe _ind au nom de la colonne si indexé
        target_col_name = f"{capital_name}_ind" if indexed else capital_name
        df = df.withColumn(target_col_name, capital_expr)

    return df


def normalize_capitals_to_100(
    df: DataFrame,
    capital_columns: List[str],
    coinsurance_col: str = 'prcdcie'
) -> DataFrame:
    """
    Normalise tous les capitaux sur une base technique de 100%.

    Règles métier :
    - Fixer PRCDCIE = 100 si manquant ou 0
    - Capital_100 = (Capital * 100) / PRCDCIE

    Paramètres :
        df : DataFrame en entrée
        capital_columns : Liste des noms de colonnes de capital à normaliser
        coinsurance_col : Colonne de pourcentage de coassurance (défaut 'prcdcie')

    Retourne :
        DataFrame avec capitaux normalisés
    """
    # Fixer PRCDCIE = 100 OÙ PRCDCIE = . ou PRCDCIE = 0
    df = df.withColumn(
        coinsurance_col,
        when(
            (col(coinsurance_col).isNull()) | (col(coinsurance_col) == 0),
            lit(100)
        ).otherwise(col(coinsurance_col))
    )

    # Normaliser chaque capital à 100%
    for capital_col in capital_columns:
        if capital_col not in df.columns:
            continue

        df = df.withColumn(
            capital_col,
            (col(capital_col) * 100) / col(coinsurance_col)
        )

    return df


def apply_capitaux_business_rules(
    df: DataFrame,
    indexed: bool = True
) -> DataFrame:
    """
    Applique les règles métier spécifiques aux Capitaux.

    Règles :
    1. Complétude SMP : SMP_100 = MAX(SMP_100, SMP_PE_100 + SMP_RD_100)
    2. Limite RC : LIMITE_RC_100 = MAX(LIMITE_RC_100_PAR_SIN, LIMITE_RC_100_PAR_AN)

    Paramètres :
        df : DataFrame en entrée
        indexed : Si True, applique aux colonnes indexées (suffixe _IND), sinon non indexées

    Retourne :
        DataFrame avec règles métier appliquées
    """
    suffix = '_ind' if indexed else ''

    # Règle 1 : Complétude SMP (16 Nov 2020)
    # SMP_100 = MAX(SMP_100, SMP_PE_100 + SMP_RD_100)
    smp_col = f'smp_100{suffix}'
    smp_pe_col = f'smp_pe_100{suffix}'
    smp_rd_col = f'smp_rd_100{suffix}'

    if all(c in df.columns for c in [smp_col, smp_pe_col, smp_rd_col]):
        df = df.withColumn(
            smp_col,
            greatest(
                col(smp_col),
                coalesce(col(smp_pe_col), lit(0.0)) + coalesce(col(smp_rd_col), lit(0.0))
            )
        )

    # Règle 2 : Limite RC - Combiner toutes les variantes RC dans une expression
    # RC_PAR_SIN inclut à la fois les correspondances directes et la variante TOUS_DOM
    # LIMITE_RC_100 = MAX(LIMITE_RC_100_PAR_SIN, LIMITE_RC_100_PAR_AN)
    rc_col = f'limite_rc_100{suffix}'
    rc_sin_col = f'limite_rc_100_par_sin{suffix}'
    rc_sin_tous_dom_col = f'limite_rc_100_par_sin_tous_dom{suffix}'
    rc_an_col = f'limite_rc_100_par_an{suffix}'

    if all(c in df.columns for c in [rc_sin_col, rc_an_col]):
        rc_sin_expr = col(rc_sin_col)

        # Inclure la variante TOUS_DOM si elle existe
        if rc_sin_tous_dom_col in df.columns:
            rc_sin_expr = greatest(
                coalesce(col(rc_sin_col), lit(0.0)),
                coalesce(col(rc_sin_tous_dom_col), lit(0.0))
            )

        df = df.withColumn(
            rc_col,
            greatest(
                coalesce(rc_sin_expr, lit(0.0)),
                coalesce(col(rc_an_col), lit(0.0))
            )
        )

    return df


def process_azec_capitals(df: DataFrame) -> DataFrame:
    """
    Traite les données de capital AZEC depuis CAPITXCU.

    Logique :
    - LCI/SMP par branche (IP0=PE, ID0=Dommages Directs)
    - Traite à la fois CAPX_100 (base 100%) et CAPX_CUA (part compagnie)
    - Agrège par POLICE + PRODUIT

    Paramètres :
        df : DataFrame CAPITXCU avec colonnes :
            - smp_sre : Type ('LCI' ou 'SMP')
            - brch_rea : Branche ('IP0' ou 'ID0')
            - capx_100 : Montant capital à 100%
            - capx_cua : Montant capital part compagnie
            - police, produit : Identifiants police et produit

    Retourne :
        DataFrame agrégé par police/produit avec :
            - smp_100_ind, lci_100_ind (base 100%)
            - smp_cie, lci_cie (part compagnie)
    """
    # =========================================================================
    # Traitement CAPX_100 (Base 100%)
    # =========================================================================

    # LCI Perte d'Exploitation (100%)
    df = df.withColumn(
        'lci_pe_100',
        when((col('smp_sre') == 'LCI') & (col('brch_rea') == 'IP0'),
             coalesce(col('capx_100'), lit(0.0))
        ).otherwise(lit(0.0))
    )

    # LCI Dommages Directs (100%)
    df = df.withColumn(
        'lci_dd_100',
        when((col('smp_sre') == 'LCI') & (col('brch_rea') == 'ID0'),
             coalesce(col('capx_100'), lit(0.0))
        ).otherwise(lit(0.0))
    )

    # SMP Perte d'Exploitation (100%)
    df = df.withColumn(
        'smp_pe_100',
        when((col('smp_sre') == 'SMP') & (col('brch_rea') == 'IP0'),
             coalesce(col('capx_100'), lit(0.0))
        ).otherwise(lit(0.0))
    )

    # SMP Dommages Directs (100%)
    df = df.withColumn(
        'smp_dd_100',
        when((col('smp_sre') == 'SMP') & (col('brch_rea') == 'ID0'),
             coalesce(col('capx_100'), lit(0.0))
        ).otherwise(lit(0.0))
    )

    # =========================================================================
    # Traitement CAPX_CUA (Part Compagnie)
    # =========================================================================

    # LCI Perte d'Exploitation (Part Compagnie)
    df = df.withColumn(
        'lci_pe_cie',
        when((col('smp_sre') == 'LCI') & (col('brch_rea') == 'IP0'),
             coalesce(col('capx_cua'), lit(0.0))
        ).otherwise(lit(0.0))
    )

    # LCI Dommages Directs (Part Compagnie)
    df = df.withColumn(
        'lci_dd_cie',
        when((col('smp_sre') == 'LCI') & (col('brch_rea') == 'ID0'),
             coalesce(col('capx_cua'), lit(0.0))
        ).otherwise(lit(0.0))
    )

    # SMP Perte d'Exploitation (Part Compagnie)
    df = df.withColumn(
        'smp_pe_cie',
        when((col('smp_sre') == 'SMP') & (col('brch_rea') == 'IP0'),
             coalesce(col('capx_cua'), lit(0.0))
        ).otherwise(lit(0.0))
    )

    # SMP Dommages Directs (Part Compagnie)
    df = df.withColumn(
        'smp_dd_cie',
        when((col('smp_sre') == 'SMP') & (col('brch_rea') == 'ID0'),
             coalesce(col('capx_cua'), lit(0.0))
        ).otherwise(lit(0.0))
    )

    # =========================================================================
    # Agrégation par POLICE + PRODUIT
    # =========================================================================
    df_agg = df.groupBy('police', 'produit').agg(
        # Agrégats base 100%
        _sum('smp_pe_100').alias('smp_pe_100'),
        _sum('smp_dd_100').alias('smp_dd_100'),
        _sum('lci_pe_100').alias('lci_pe_100'),
        _sum('lci_dd_100').alias('lci_dd_100'),
        # Agrégats part compagnie
        _sum('smp_pe_cie').alias('smp_pe_cie'),
        _sum('smp_dd_cie').alias('smp_dd_cie'),
        _sum('lci_pe_cie').alias('lci_pe_cie'),
        _sum('lci_dd_cie').alias('lci_dd_cie')
    )

    # Calcul des totaux (Base 100%)
    df_agg = df_agg.withColumn(
        'smp_100_ind',
        col('smp_pe_100') + col('smp_dd_100')
    )

    df_agg = df_agg.withColumn(
        'lci_100_ind',
        col('lci_pe_100') + col('lci_dd_100')
    )

    # Calcul des totaux (Part Compagnie)
    df_agg = df_agg.withColumn(
        'smp_cie',
        col('smp_pe_cie') + col('smp_dd_cie')
    )

    df_agg = df_agg.withColumn(
        'lci_cie',
        col('lci_pe_cie') + col('lci_dd_cie')
    )

    # Renommer pour correspondre au schéma attendu
    df_agg = df_agg.withColumnRenamed('police', 'nopol')
    df_agg = df_agg.withColumnRenamed('produit', 'cdprod')

    return df_agg


def aggregate_azec_pe_rd(df: DataFrame) -> DataFrame:
    """
    Agrège les données AZEC Perte d'Exploitation et Risque Direct.

    Logique :
    - Somme MT_BASPE et MT_BASDI par POLICE + PRODUIT

    Paramètres :
        df : DataFrame INCENDCU avec colonnes :
            - police, produit : Identifiants
            - mt_baspe : Montant perte d'exploitation
            - mt_basdi : Montant dommages directs

    Retourne :
        DataFrame agrégé avec :
            - perte_exp_100_ind : Somme de PE
            - risque_direct_100_ind : Somme de RD
            - value_insured_100_ind : PE + RD
    """
    df_agg = df.groupBy('police', 'produit').agg(
        _sum(coalesce(col('mt_baspe'), lit(0.0))).alias('perte_exp_100_ind'),
        _sum(coalesce(col('mt_basdi'), lit(0.0))).alias('risque_direct_100_ind')
    )

    df_agg = df_agg.withColumn(
        'value_insured_100_ind',
        col('perte_exp_100_ind') + col('risque_direct_100_ind')
    )

    # Renommer pour correspondre au schéma attendu
    df_agg = df_agg.withColumnRenamed('police', 'nopol')
    df_agg = df_agg.withColumnRenamed('produit', 'cdprod')

    return df_agg
