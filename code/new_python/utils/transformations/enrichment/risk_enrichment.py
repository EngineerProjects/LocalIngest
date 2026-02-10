"""
Utilitaires d'enrichissement RISK pour le processeur de consolidation.

Fournit des fonctions réutilisables pour la jointure et le fusionnement (coalesce) des données de risque IRD
(Q45, Q46, QAN) afin d'éviter la duplication de code.
"""

from typing import List, Dict, Optional, Any
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, when, to_date, broadcast, substring, row_number
from pyspark.sql.types import StringType, DateType
from pyspark.sql.window import Window


def _parse_sas_datetime_to_date(col_expr):
    """
    Convertit une chaîne SAS DATETIME '14MAY1991:00:00:00' → PySpark DATE '1991-05-14'.

    Équivalent à la fonction SAS datepart().
    Extrait les 9 premiers caractères et les analyse avec le format de date SAS.

    Args:
        col_expr: Expression de colonne PySpark contenant la chaîne SAS DATETIME

    Returns:
        Colonne PySpark avec une valeur DateType

    Exemples:
        >>> _parse_sas_datetime_to_date(col("dtouchan"))
        # '14MAY1991:00:00:00' → date('1991-05-14')
        # '04MAR2012:00:00:00' → date('2012-03-04')
    """
    # Extrait les 9 premiers caractères : '14MAY1991:00:00:00' → '14MAY1991'
    # Parse avec le format datetime SAS : ddMMMyyyy
    return to_date(substring(col_expr, 1, 9), "ddMMMyyyy")


def enrich_with_risk_data(
    df: DataFrame,
    risk_sources: List[str],
    vision: str,
    bronze_reader: Any,
    logger: Optional[Any] = None
) -> DataFrame:
    """
    Enrichissement générique des données de risque pour plusieurs sources IRD (Q45/Q46/QAN).

    Élimine la duplication de code en traitant toutes les sources avec la même logique.
    Suit le modèle SAS séquentiel 'join-coalesce-drop'.

    Args:
        df: DataFrame principal à enrichir
        risk_sources: Liste des noms de sources ['q46', 'q45', 'qan']
        vision: Vision au format YYYYMM
        bronze_reader: Instance de BronzeReader pour lire les fichiers IRD
        logger: Instance de logger optionnelle

    Returns:
        DataFrame enrichi avec toutes les sources RISK spécifiées

    Exemple:
        >>> df = enrich_with_risk_data(
        ...     df, ['q46', 'q45', 'qan'], '202509', reader, logger
        ... )
    """
    # Configuration pour chaque source IRD
    IRD_CONFIG = {
        'q46': {
            'file_group': 'ird_risk_q46',
            'date_columns': ['dtouchan', 'dtrectrx', 'dtreffin'],
            'text_columns': ['ctprvtrv', 'ctdeftra', 'lbnattrv', 'lbdstcsc']
        },
        'q45': {
            'file_group': 'ird_risk_q45',
            'date_columns': ['dtouchan', 'dtrectrx', 'dtreffin'],
            'text_columns': ['ctprvtrv', 'ctdeftra', 'lbnattrv', 'lbdstcsc']
        },
        'qan': {
            'file_group': 'ird_risk_qan',
            'date_columns': ['dtouchan', 'dtrcppr'],
            'text_columns': ['ctprvtrv', 'ctdeftra', 'lbnattrv', 'dstcsc']
        }
    }

    # Initialiser dtreffin si nécessaire
    if 'dtreffin' not in df.columns:
        from utils.processor_helpers import add_null_columns
        df = add_null_columns(df, {'dtreffin': DateType})

    # Traiter chaque source séquentiellement (modèle SAS)
    for source in risk_sources:
        if source not in IRD_CONFIG:
            if logger:
                logger.warning(f"Source RISK inconnue : {source}, ignorée")
            continue

        df = _join_single_risk_source(
            df, IRD_CONFIG[source], source, vision, bronze_reader, logger
        )

    if logger:
        logger.info(f"Enrichissement RISK terminé pour les sources : {risk_sources}")

    return df


def _join_single_risk_source(
    df: DataFrame,
    config: Dict,
    source_name: str,
    vision: str,
    bronze_reader: Any,
    logger: Optional[Any] = None
) -> DataFrame:
    """
    Jointure d'une seule source IRD, fusionne les valeurs, supprime les colonnes temporaires.

    Fonction interne pour enrich_with_risk_data().

    Args:
        df: DataFrame principal
        config: Configuration de la source (file_group, date_columns, text_columns)
        source_name: Nom de la source pour la journalisation ('q45', 'q46', 'qan')
        vision: Chaîne vision
        bronze_reader: Instance BronzeReader
        logger: Logger optionnel

    Returns:
        DataFrame avec les données IRD fusionnées et les colonnes _risk supprimées
    """
    try:
        # Lire le fichier IRD
        df_ird = bronze_reader.read_file_group(config['file_group'], vision)

        if df_ird is None or df_ird.isEmpty(): # Optimisation: isEmpty() est préférable à count() == 0
            if logger:
                logger.debug(f"IRD {source_name.upper()} : Aucune donnée trouvée, ignoré")
            return df

        # Dédupliquer sur NOPOL si nécessaire
        df_ird = _deduplicate_on_nopol(
            df_ird, config['date_columns'], source_name, logger
        )

        # Préparer les colonnes IRD avec le suffixe _risk
        select_cols = ['nopol']

        for col_name in config['date_columns']:
            if col_name in df_ird.columns:
                # Parse le format SAS DATETIME '14MAY1991:00:00:00' → DATE
                # Équivalent à la fonction SAS datepart()
                select_cols.append(
                    _parse_sas_datetime_to_date(col(col_name)).alias(f"{col_name}_risk")
                )

        for col_name in config['text_columns']:
            if col_name in df_ird.columns:
                select_cols.append(col(col_name).alias(f"{col_name}_risk"))
            else:
                select_cols.append(lit(None).cast(StringType()).alias(f"{col_name}_risk"))

        df_ird_select = df_ird.select(*select_cols)

        # Left join
        df = df.join(broadcast(df_ird_select), on='nopol', how='left')

        # Fusionner les valeurs (Coalesce)
        df = _coalesce_risk_columns(
            df, config['date_columns'], config['text_columns'], source_name
        )

        # Supprimer les colonnes temporaires _risk
        risk_cols_to_drop = [
            f"{c}_risk" for c in config['date_columns'] + config['text_columns']
        ]
        risk_cols_existing = [c for c in risk_cols_to_drop if c in df.columns]
        if risk_cols_existing:
            df = df.drop(*risk_cols_existing)

        if logger:
            logger.debug(f"IRD {source_name.upper()} : Jointure et fusion réussies")

    except Exception as e:
        if logger:
            logger.warning(f"Échec de l'enrichissement IRD {source_name.upper()} : {e}")

    return df


def _deduplicate_on_nopol(
    df_ird: DataFrame,
    date_columns: List[str],
    source_name: str,
    logger: Optional[Any] = None
) -> DataFrame:
    """
    Supprime les doublons NOPOL des données IRD, en conservant l'entrée la plus récente.

    Args:
        df_ird: DataFrame IRD à dédupliquer
        date_columns: Colonnes de date à utiliser pour l'ordre (le plus récent gagne)
        source_name: Nom de la source pour la journalisation
        logger: Logger optionnel

    Returns:
        DataFrame dédupliqué
    """
    # Construire l'ordre par dates les plus récentes (nulls à la fin)
    order_cols = []
    for date_col in date_columns:
        if date_col in df_ird.columns:
            order_cols.append(col(date_col).desc_nulls_last())

    if not order_cols:
        order_cols = [lit(1)]  # Fallback si pas de dates

    window_spec = Window.partitionBy("nopol").orderBy(*order_cols)
    
    # Conserver uniquement la première ligne (la plus récente)
    df_ird = (df_ird
              .withColumn("_row_num", row_number().over(window_spec))
              .filter(col("_row_num") == 1)
              .drop("_row_num"))

    if logger:
        # On ne logue plus le nombre exact de doublons pour éviter un count() coûteux
        logger.debug(f"IRD {source_name.upper()} : Déduplication appliquée")

    return df_ird


def _coalesce_risk_columns(
    df: DataFrame,
    date_columns: List[str],
    text_columns: List[str],
    source_name: str
) -> DataFrame:
    """
    Fusionne (coalesce) les colonnes de risque avec les colonnes du DataFrame principal.

    Args:
        df: DataFrame principal avec les colonnes _risk issues de la jointure
        date_columns: Colonnes de date à fusionner
        text_columns: Colonnes de texte à fusionner
        source_name: Nom de la source pour logique spéciale

    Returns:
        DataFrame avec valeurs fusionnées
    """
    for col_name in date_columns + text_columns:
        risk_col = f"{col_name}_risk"

        # Logique métier :
        # Q45/Q46 -> lbdstcsc mappe vers dstcsc
        # QAN     -> dstcsc mappe vers dstcsc
        if col_name == 'lbdstcsc':
            target_col = 'dstcsc'
        elif source_name == 'qan' and col_name == 'dstcsc':
            target_col = 'dstcsc'
        else:
            target_col = col_name

        if risk_col in df.columns:

            # Règle spéciale : Q46 écrase DTREFFIN sans condition
            if col_name == 'dtreffin' and source_name == 'q46':
                df = df.withColumn('dtreffin', col(risk_col))

            else:
                # Sinon, remplir si NULL (coalesce, mais préservant la colonne existante si non nulle)
                df = df.withColumn(
                    target_col,
                    when(col(target_col).isNull(), col(risk_col))
                    .otherwise(col(target_col))
                )

    return df
