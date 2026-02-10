"""
Opérations de transformation spécifiques aux Émissions.

Assistants simples pour :
- Affectation du canal de distribution (CDPOLE) à partir de CD_NIV_2_STC
- Calcul de la répartition de l'exercice (courant vs antérieur)
- Extraction du code de garantie (CGARP)
- Applications des filtres métiers
"""

from pyspark.sql import DataFrame  # type: ignore
from pyspark.sql.functions import (  # type: ignore
    col, when, lit, substring, regexp_replace, sum as spark_sum, coalesce
)
from typing import Dict, Any, Optional
from config.constants import MARKET_CODE


def assign_distribution_channel(df: DataFrame) -> DataFrame:
    """
    Affecte le canal de distribution (CDPOLE) basé sur CD_NIV_2_STC.

    Règles métier :
    - DCAG, DCPS, DIGITAL → '1' (Agent)
    - BROKDIV → '3' (Courtage)

    Paramètres :
        df : DataFrame en entrée avec la colonne cd_niv_2_stc

    Retourne :
        DataFrame avec la colonne cdpole ajoutée
    """
    df = df.withColumn('cdpole',
        when(col('cd_niv_2_stc').isin(['DCAG', 'DCPS', 'DIGITAL']), lit('1'))
        .when(col('cd_niv_2_stc') == 'BROKDIV', lit('3'))
        .otherwise(lit(None))
    )
    return df


def calculate_exercice_split(
    df: DataFrame,
    vision: str,
    year_col: str = 'nu_ex_ratt_cts'
) -> DataFrame:
    """
    Répartit les primes en exercice courant ('cou') vs exercice antérieur ('ant').

    Logique :
    - Si année de rattachement >= année de vision → 'cou' (courant)
    - Sinon → 'ant' (antérieur)

    Paramètres :
        df : DataFrame en entrée
        vision : Vision au format YYYYMM
        year_col : Colonne contenant l'année de rattachement

    Retourne :
        DataFrame avec la colonne exercice ajoutée
    """
    from utils.helpers import extract_year_month_int
    year_int, _ = extract_year_month_int(vision)

    df = df.withColumn('exercice',
        when(col(year_col) >= lit(str(year_int)), lit('cou'))
        .otherwise(lit('ant'))
    )
    return df


def extract_guarantee_code(
    df: DataFrame,
    source_col: str = 'cd_gar_prospctiv',
    target_col: str = 'cgarp'
) -> DataFrame:
    """
    Extrait le code de garantie du champ de garantie prospective.

    Logique :
    - Extrait les caractères 3 à 5 de cd_gar_prospctiv
    - Supprime les espaces (compress)

    Paramètres :
        df : DataFrame en entrée
        source_col : Nom de la colonne source
        target_col : Nom de la colonne cible pour le code extrait

    Retourne :
        DataFrame avec le code de garantie extrait
    """
    df = df.withColumn(target_col,
        regexp_replace(
            substring(col(source_col), 3, 5),
            r'\s+', ''
        )
    )
    return df


def apply_emissions_filters(
    df: DataFrame,
    config: Dict[str, Any],
    vision: str,
    logger: Optional[Any] = None
) -> DataFrame:
    """
    Applique tous les filtres métier Émissions.

    Filtres :
    1. Filtre marché construction (cd_marche = '6')
    2. Filtre date comptable (dt_cpta_cts <= vision)
    3. Intermédiaires exclus
    4. Exclusions produit/garantie
    5. Exclusions catégories

    Paramètres :
        df : DataFrame en entrée
        config : Dictionnaire de configuration Émissions
        vision : Vision au format YYYYMM
        logger : Instance de logger optionnelle

    Retourne :
        DataFrame filtré
    """
    initial_count = df.count() if logger else 0

    # Filtre pour le marché construction uniquement (cd_marche='6')
    df = df.filter(col('cd_marche') == MARKET_CODE.MARKET)
    if logger:
        logger.info(f"Après filtre marché (cd_marche='6') : {df.count():,} enregistrements")

    # Filtre 2 : Date comptable <= vision
    df = df.filter(col('dt_cpta_cts') <= lit(vision))
    if logger:
        logger.info(f"Après filtre date (dt_cpta_cts <= {vision}) : {df.count():,} enregistrements")

    # Filtre 3 : Intermédiaires exclus
    excluded_noint_config = config.get('excluded_intermediaries', {})
    # Structure config : {"description": "...", "values": [...]}
    excluded_noint = excluded_noint_config.get('values', []) if isinstance(excluded_noint_config, dict) else excluded_noint_config
    if excluded_noint:
        df = df.filter(~col('cd_int_stc').isin(excluded_noint))
        if logger:
            logger.info(f"Après filtre intermédiaires ({len(excluded_noint)} exclus) : {df.count():,} enregistrements")

    # Filtre 4 : Exclusions produit/garantie
    product_guarantee_config = config.get('product_guarantee_exclusions', {})
    product_guarantee_rules = product_guarantee_config.get('rules', []) if isinstance(product_guarantee_config, dict) else product_guarantee_config
    for exclusion in product_guarantee_rules:
        if 'product_prefix' in exclusion:
            # Préfixe produit avec garantie
            df = df.filter(
                ~((substring(col('cd_prd_prm'), 1, 2) == exclusion['product_prefix']) &
                  (col('cd_gar_prospctiv') == exclusion['guarantee']))
            )
        elif 'intermediary' in exclusion:
            # Intermédiaire avec produit
            df = df.filter(
                ~((col('cd_int_stc') == exclusion['intermediary']) &
                  (col('cd_prd_prm') == exclusion['product']))
            )

    # Filtre 5 : Garanties exclues
    excluded_guarantees_config = config.get('excluded_guarantees', {})
    excluded_guarantees = excluded_guarantees_config.get('values', []) if isinstance(excluded_guarantees_config, dict) else excluded_guarantees_config
    if excluded_guarantees:
        df = df.filter(~col('cd_gar_prospctiv').isin(excluded_guarantees))

    # Filtre 6 : Produit exclu
    excluded_product_config = config.get('excluded_product', {})
    excluded_product = excluded_product_config.get('value') if isinstance(excluded_product_config, dict) else excluded_product_config
    if excluded_product:
        df = df.filter(col('cd_prd_prm') != excluded_product)

    # Filtre 7 : Catégories exclues et catégorie de garantie
    excluded_categories_config = config.get('excluded_categories', {})
    excluded_categories = excluded_categories_config.get('values', []) if isinstance(excluded_categories_config, dict) else excluded_categories_config
    excluded_gar_cat_config = config.get('excluded_guarantee_category', {})
    excluded_gar_cat = excluded_gar_cat_config.get('value') if isinstance(excluded_gar_cat_config, dict) else excluded_gar_cat_config

    if excluded_categories or excluded_gar_cat:
        filter_expr = lit(False)
        if excluded_categories:
            filter_expr = filter_expr | col('cd_cat_min').isin(excluded_categories)
        if excluded_gar_cat:
            filter_expr = filter_expr | (col('cd_gar_prospctiv') == excluded_gar_cat)

        df = df.filter(~filter_expr)

    if logger:
        final_count = df.count()
        filtered_out = initial_count - final_count
        logger.success(f"Filtres appliqués : {filtered_out:,} enregistrements filtrés, {final_count:,} restants")

    return df


def aggregate_by_policy_guarantee(
    df: DataFrame,
    group_cols: list
) -> DataFrame:
    """
    Agrège les primes et commissions selon les colonnes de regroupement spécifiées.

    Logique :
    - Regroupe par vision, dircom, cdpole, nopol, cdprod, noint, cgarp, cmarch, cseg, cssseg, cd_cat_min
    - Somme : primes_x, primes_n, mtcom_x

    Paramètres :
        df : DataFrame en entrée avec les données de primes
        group_cols : Liste des colonnes de regroupement

    Retourne :
        DataFrame agrégé
    """
    from pyspark.sql.functions import sum as _sum

    df_agg = df.groupBy(*group_cols).agg(
        _sum('mt_ht_cts').alias('primes_x'),
        _sum('primes_n').alias('primes_n'),
        _sum('mtcom').alias('mtcom_x')
    )

    return df_agg
