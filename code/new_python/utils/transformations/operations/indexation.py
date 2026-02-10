"""
Indexation des capitaux pour le Pipeline de Données Construction.

Implémente la logique d'indexation des capitaux basée sur les indices du coût de la construction.
Indexe les montants de capital en fonction des coefficients d'évolution.

Logique métier :
  - CAS 1 (DATE non spécifiée) : Utiliser les coefficients PRPRVC existants (indices 1ère année)
  - CAS 2 (DATE spécifiée) : Rechercher les indices dans la table INDICES via le format
"""

from pyspark.sql import DataFrame # type: ignore
from pyspark.sql.functions import ( # type: ignore
    col, when, lit, year, month, dayofmonth, current_date,
    date_format, concat, lpad, to_date, coalesce, substring
)
from typing import Optional, Any, Dict


def index_capitals(
    df: DataFrame,
    num_capitals: int = 14,
    date_col: str = "dtechamm",
    contract_start_col: str = "dtefsitt",
    capital_prefix: str = "mtcapi",
    nature_prefix: str = "cdprvb",
    index_prefix: str = "prprvc",
    reference_date: Optional[str] = None,
    index_table_df: Optional[DataFrame] = None,
    use_index_table: bool = True,
    logger: Optional[Any] = None
) -> DataFrame:
    """
    Indexe les montants de capitaux en utilisant les indices du coût de la construction.

    Implémente deux modes distincts :

    CAS 1 (use_index_table=False) :
      - Utilise les coefficients PRPRVC existants comme indices d'origine (indices 1ère année)
      - Pas de recherche dans la table INDICES
      - Indice cible = 1.0 (pas de ré-indexation)
      - Formule : mtcapi_indexed = mtcapi / prprvc (désindexation vers l'année de base)

    CAS 2 (use_index_table=True) :
      - Recherche l'indice d'origine utilisant CDPRVB + DTEFSITT dans la table INDICES
      - Recherche l'indice cible utilisant CDPRVB + reference_date dans la table INDICES
      - Formule : mtcapi_indexed = mtcapi * (target_index / origin_index)

    Paramètres :
        df : DataFrame en entrée
        num_capitals : Nombre de colonnes de capital à indexer (défaut 14)
        date_col : Colonne date anniversaire (défaut 'dtechamm')
        contract_start_col : Date de début de contrat (défaut 'dtefsitt')
        capital_prefix : Préfixe de colonne montant capital (défaut 'mtcapi')
        nature_prefix : Préfixe code nature provision (défaut 'cdprvb')
        index_prefix : Préfixe colonne indice depuis données existantes (défaut 'prprvc')
        reference_date : Date de référence pour l'indexation (format YYYY-MM-DD)
                       Si None, utilise la date courante
        index_table_df : Table de référence des indices optionnelle (requise pour CAS 2)
        use_index_table : Si True, utilise CAS 2 (recherche dans INDICES). Si False, utilise CAS 1 (PRPRVC)
        logger : Instance de logger optionnelle

    Retourne :
        DataFrame avec colonnes de capital indexées ajoutées (mtcapi1i, mtcapi2i, ..., mtcapi14i)
        et colonnes de suivi d'indice (indxorig1i, indxintg1i, ..., indxorig14i, indxintg14i)
    """
    if logger:
        logger.info(f"Démarrage de l'indexation des capitaux pour {num_capitals} colonnes")

    # Déterminer la date de référence (utiliser la date courante si non spécifiée)
    if reference_date is None:
        reference_date_col = current_date()
    else:
        reference_date_col = lit(reference_date)

    # Indexer chaque colonne de capital
    for i in range(1, num_capitals + 1):
        capital_col = f"{capital_prefix}{i}"
        nature_col = f"{nature_prefix}{i}"
        index_col = f"{index_prefix}{i}"  # Coefficient d'évolution existant depuis les données source
        indexed_col = f"{capital_col}i"
        index_origin_col = f"indxorig{i}i"
        index_target_col = f"indxintg{i}i"

        # Vérifier si la colonne de capital existe
        if capital_col not in df.columns:
            if logger:
                logger.debug(f"Ignoré {capital_col} - colonne non trouvée")
            continue

        if logger:
            logger.debug(f"Indexation de {capital_col}")

        # =====================================================================
        # DÉTERMINER LA DATE ANNIVERSAIRE (Date Cible)
        # Extrait mois/jour de DTECHANN, construit la date anniversaire
        # =====================================================================

        # DTECHANN est au format IntegerType MMJJ (ex: 1231 = 31 décembre)
        # Extraction arithmétique : MOIS = DTECHANN / 100, JOUR = DTECHANN % 100
        df = df.withColumn(
            f"_temp_anniv_month_{i}",
            (col(date_col) / lit(100)).cast("int")  # MMJJ / 100 = MM
        )
        df = df.withColumn(
            f"_temp_anniv_day_{i}",
            (col(date_col) % lit(100)).cast("int")  # MMJJ % 100 = JJ
        )

        # Construit la date anniversaire avec l'année de référence
        df = df.withColumn(
            f"_temp_anniv_date_{i}",
            to_date(
                concat(
                    year(reference_date_col),
                    lit("-"),
                    lpad(col(f"_temp_anniv_month_{i}"), 2, "0"),
                    lit("-"),
                    lpad(col(f"_temp_anniv_day_{i}"), 2, "0")
                )
            )
        )

        # Ajuster l'année si date anniversaire > date référence
        df = df.withColumn(
            f"_temp_target_date_{i}",
            when(
                col(f"_temp_anniv_date_{i}") > reference_date_col,
                to_date(
                    concat(
                        year(reference_date_col) - 1,
                        lit("-"),
                        lpad(col(f"_temp_anniv_month_{i}"), 2, "0"),
                        lit("-"),
                        lpad(col(f"_temp_anniv_day_{i}"), 2, "0")
                    )
                )
            ).otherwise(col(f"_temp_anniv_date_{i}"))
        )

        # =====================================================================
        # RÉCUPÉRER LES INDICES - DEUX CAS BASÉS SUR LA LOGIQUE MÉTIER
        # =====================================================================

        if not use_index_table or index_table_df is None:
            # ═════════════════════════════════════════════════════════════════
            # CAS 1 : Utiliser les coefficients PRPRVC existants
            # ═════════════════════════════════════════════════════════════════
            if index_col in df.columns:
                # Ces sont les "coefficients d'évolution" = indices 1ère année
                df = df.withColumn(
                    index_origin_col,
                    coalesce(col(index_col), lit(1.0))
                )
                # Pas de recherche d'indice cible dans ce cas
                # Cible est implicitement 1.0 (montant indexé = original / prprvc)
                df = df.withColumn(
                    index_target_col,
                    lit(1.0)
                )

                if logger:
                    logger.debug(f"{capital_col}: CAS 1 - Utilisation directe de PRPRVC{i} (pas de recherche INDICES)")
            else:
                # Pas de données d'indice disponibles - utiliser ratio de 1.0 (pas d'indexation)
                df = df.withColumn(index_origin_col, lit(1.0))
                df = df.withColumn(index_target_col, lit(1.0))

                if logger:
                    logger.debug(f"{capital_col}: Colonne PRPRVC non trouvée - pas d'indexation")

        elif nature_col in df.columns:
            # ═════════════════════════════════════════════════════════════════
            # CAS 2 : Rechercher les indices dans la table INDICES
            # ═════════════════════════════════════════════════════════════════
            if logger:
                logger.debug(f"{capital_col}: CAS 2 - Recherche des indices dans la table INDICES")

            # Construit la clé de recherche pour l'indice d'origine : code nature + DTEFSITT
            df = df.withColumn(
                f"_temp_origin_key_{i}",
                concat(
                    col(nature_col),
                    date_format(col(contract_start_col), "MMddyy")  # TODO: Vérifier le format
                )
            )

            # Construit la clé de recherche pour l'indice cible : code nature + date cible
            df = df.withColumn(
                f"_temp_target_key_{i}",
                concat(
                    col(nature_col),
                    date_format(col(f"_temp_target_date_{i}"), "MMddyy")  # TODO: Vérifier le format
                )
            )

            # Jointure pour l'indice d'origine
            index_origin_alias = f"idx_orig_{i}"
            df = df.join(
                index_table_df.select(
                    col("index_key").alias(f"_origin_key_{i}"),
                    col("index_value").alias(index_origin_alias)
                ),
                df[f"_temp_origin_key_{i}"] == col(f"_origin_key_{i}"),
                "left"
            ).drop(f"_origin_key_{i}")

            # Jointure pour l'indice cible
            index_target_alias = f"idx_target_{i}"
            df = df.join(
                index_table_df.select(
                    col("index_key").alias(f"_target_key_{i}"),
                    col("index_value").alias(index_target_alias)
                ),
                df[f"_temp_target_key_{i}"] == col(f"_target_key_{i}"),
                "left"
            ).drop(f"_target_key_{i}")

            # Utiliser l'indice seulement si le code nature commence par '0'
            df = df.withColumn(
                index_origin_col,
                when(
                    substring(col(nature_col), 1, 1) == "0",
                    coalesce(col(index_origin_alias), lit(1.0))
                ).otherwise(lit(1.0))
            ).drop(index_origin_alias)

            df = df.withColumn(
                index_target_col,
                when(
                    substring(col(nature_col), 1, 1) == "0",
                    coalesce(col(index_target_alias), lit(1.0))
                ).otherwise(lit(1.0))
            ).drop(index_target_alias)

            # Nettoyer les colonnes de clés temporaires
            df = df.drop(f"_temp_origin_key_{i}", f"_temp_target_key_{i}")

        else:
            # Colonne nature non trouvée - défaut à pas d'indexation
            df = df.withColumn(index_origin_col, lit(1.0))
            df = df.withColumn(index_target_col, lit(1.0))

            if logger:
                logger.warning(f"{capital_col}: Colonne nature {nature_col} non trouvée - pas d'indexation")

        # =====================================================================
        # APPLIQUER LA LOGIQUE D'INDEXATION
        # Pas d'indexation si date cible < date origine
        # =====================================================================

        df = df.withColumn(
            index_origin_col,
            when(
                col(f"_temp_target_date_{i}") < col(contract_start_col),
                lit(1.0)
            ).otherwise(col(index_origin_col))
        )

        df = df.withColumn(
            index_target_col,
            when(
                col(f"_temp_target_date_{i}") < col(contract_start_col),
                lit(1.0)
            ).otherwise(col(index_target_col))
        )

        # =====================================================================
        # CALCULER LE MONTANT DE CAPITAL INDEXÉ
        # Formule : mtcapi_indexed = mtcapi * (target_index / origin_index)
        # =====================================================================

        df = df.withColumn(
            indexed_col,
            when(
                (col(index_target_col).isNull()) |
                (col(index_origin_col).isNull()) |
                (col(index_origin_col) == 0),
                col(capital_col)  # Pas d'indexation si indices invalides
            ).otherwise(
                col(capital_col) * (col(index_target_col) / col(index_origin_col))
            )
        )

        # Nettoyer les colonnes temporaires
        df = df.drop(
            f"_temp_anniv_month_{i}",
            f"_temp_anniv_day_{i}",
            f"_temp_anniv_date_{i}",
            f"_temp_target_date_{i}"
        )

    if logger:
        logger.success(f"Indexation des capitaux terminée pour {num_capitals} colonnes")

    return df


def load_index_table(
    spark,
    config,
    logger: Optional[Any] = None
) -> Optional[DataFrame]:
    """
    Charge la table de référence des indices du coût de la construction.

    La table d'index doit avoir la structure :
    - index_key : Concaténation de code nature + date (format MMDDYY)
                  Exemple : "0110925" = code nature "01" + date "10/09/25"
    - index_value : Coefficient d'indice du coût de la construction

    Paramètres :
        spark : SparkSession
        config : Instance ConfigLoader
        logger : Logger optionnel

    Retourne :
        DataFrame avec données de référence d'index, ou None si non disponible
    """
    if logger:
        logger.info("Chargement de la table des indices du coût de la construction")

    try:
        from src.reader import BronzeReader

        reader = BronzeReader(spark, config)

        # Essayer de charger la table d'index depuis bronze/ref/
        index_df = reader.read_file_group("indices", vision=None)

        if index_df is not None:
            if logger:
                logger.success(f"Table des indices chargée")
            return index_df
        else:
            if logger:
                logger.warning("La table des indices est vide")
            return None

    except Exception as e:
        if logger:
            logger.warning(f"Impossible de charger la table des indices : {e}")
            logger.info("L'indexation utilisera un ratio par défaut de 1.0 (pas d'indexation)")
        return None


def create_indexed_capital_sums(
    df: DataFrame,
    num_capitals: int = 14,
    capital_prefix: str = "mtcapi",
    label_prefix: str = "lblcap",
    target_columns: Optional[Dict[str, list]] = None,
    logger: Optional[Any] = None
) -> DataFrame:
    """
    Crée la somme des capitaux indexés pour des catégories spécifiques.

    Similaire à extract_capitals mais utilisant les valeurs indexées (mtcapi1i, mtcapi2i, ...).

    Paramètres :
        df : DataFrame en entrée avec capitaux indexés
        num_capitals : Nombre de colonnes de capital
        capital_prefix : Préfixe de colonne capital indexé (défaut 'mtcapi')
        label_prefix : Préfixe de colonne libellé pour correspondance (défaut 'lblcap')
        target_columns : Dictionnaire de noms de colonnes cibles vers mots-clés de libellé
                       Exemple : {'smp_100_indexed': ['SMP GLOBAL', 'SMP RETENU']}
        logger : Logger optionnel

    Retourne :
        DataFrame avec colonnes de somme de capital indexé
    """
    if target_columns is None:
        target_columns = {}

    if logger:
        logger.info("Création des sommes de capitaux indexés")

    # Pour chaque colonne cible, sommer les capitaux indexés correspondant aux mots-clés
    for target_col, keywords in target_columns.items():
        sum_expr = lit(0.0)

        for i in range(1, num_capitals + 1):
            indexed_col = f"{capital_prefix}{i}i"
            label_col = f"{label_prefix}{i}"

            if indexed_col not in df.columns:
                continue

            # Construire la condition : libellé correspond à n'importe quel mot-clé
            if label_col in df.columns:
                match_condition = lit(False)
                for keyword in keywords:
                    match_condition = match_condition | col(label_col).contains(keyword)

                # Ajouter à la somme si correspondance
                sum_expr = sum_expr + when(match_condition, coalesce(col(indexed_col), lit(0.0))).otherwise(lit(0.0))

        df = df.withColumn(target_col, sum_expr)

    if logger:
        logger.success(f"Création de {len(target_columns)} colonnes de somme de capital indexé")

    return df
