"""
Utilitaires de transformation au niveau des colonnes.

Ce module gère :
- La sélection des colonnes (projection)
- Le renommage optionnel
- Les colonnes calculées simples (opérations sûres uniquement)
- L'initialisation des colonnes avec types
- L'injection de métadonnées (vision, année, mois)

Important :
Ce fichier ne supporte intentionnellement PAS les expressions complexes,
l'arithmétique de dates, la logique imbriquée ou toute opération dépendant
d'un ordre de mise à jour spécifique. Ces opérations DOIVENT rester dans le code PySpark,
et non dans les fichiers de configuration.

Tous les noms de colonnes en sortie restent en minuscules.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, coalesce
from pyspark.sql.types import DataType


# =========================================================
# API Publique
# =========================================================

def lowercase_all_columns(df: DataFrame) -> DataFrame:
    """
    Convertit tous les noms de colonnes du DataFrame en minuscules.

    Notes :
        Pour assurer la cohérence à travers le pipeline,
        toutes les colonnes sont normalisées en minuscules.
    """
    return df.select([col(c).alias(c.lower()) for c in df.columns])


def apply_column_config(
    df: DataFrame,
    config: dict,
    vision: str = None,
    annee: int = None,
    mois: int = None
) -> DataFrame:
    """
    Applique une configuration structurée de sélection et d'initialisation des colonnes.

    Actions supportées :
    - Passthrough : conserve les colonnes présentes dans le DataFrame
    - Renommage : mappage simple ancien_nom -> nouveau_nom
    - Colonnes calculées : opérations très simples uniquement (coalesce, défaut)
    - Initialisation : création de colonnes manquantes avec valeurs typées par défaut
    - Métadonnées : ajout de VISION, EXEVUE, MOISVUE

    Cette fonction évite explicitement :
        - tout eval()
        - toute analyse de condition complexe
        - toute évaluation d'expression non sûre
        - toute logique arithmétique multi-colonnes

    Paramètres
    ----------
    df : DataFrame
        DataFrame en entrée avec noms de colonnes en minuscules.
    config : dict
        Section issue du JSON de transformations.
    vision : str
        Chaîne YYYYMM.
    annee : int
        Année extraite de la vision.
    mois : int
        Mois extrait de la vision.

    Retour
    -------
    DataFrame
        DataFrame transformé avec colonnes sélectionnées / initialisées.
    """

    select_exprs = []

    # ---------------------------
    # 2. Colonnes renommées
    # ---------------------------
    rename_map = {old.lower(): new.lower() for old, new in config.get("rename", {}).items()}

    for orig_col in df.columns:
        lc = orig_col.lower()
        if lc in rename_map:
            select_exprs.append(col(orig_col).alias(rename_map[lc]))
        else:
            select_exprs.append(col(orig_col))

    # ---------------------------
    # 3. Colonnes calculées (sûres)
    # ---------------------------
    for col_name, comp_cfg in config.get("computed", {}).items():
        expr = _safe_computed_expression(df, comp_cfg)
        select_exprs.append(expr.alias(col_name.lower()))

    # ---------------------------
    # 4. Initialisation colonnes
    # ---------------------------
    for col_name, (default_val, dtype) in config.get("init", {}).items():
        cname = col_name.lower()
        if isinstance(dtype, DataType):
            select_exprs.append(lit(default_val).cast(dtype).alias(cname))
        else:
            select_exprs.append(lit(default_val).alias(cname))

    # ---------------------------
    # 5. Colonnes de métadonnées
    # ---------------------------
    if vision is not None:
        select_exprs.append(lit(str(vision)).alias("vision"))
    if annee is not None:
        select_exprs.append(lit(int(annee)).alias("exevue"))
    if mois is not None:
        select_exprs.append(lit(int(mois)).alias("moisvue"))

    # ---------------------------
    # 6. Appliquer SELECT d'abord
    # ---------------------------
    df = df.select(*select_exprs)

    # ---------------------------
    # 7. Puis DROP colonnes
    # ---------------------------
    drop_set = set(d.lower() for d in config.get("drop", []))
    if drop_set:
        drop_existing = [c for c in drop_set if c in df.columns]
        if drop_existing:
            df = df.drop(*drop_existing)

    return df


# =========================================================
# Helpers Internes
# =========================================================

def _safe_computed_expression(df: DataFrame, cfg: dict):
    """
    Construit une expression de colonne 'calculée' sûre.

    Types supportés :
    - coalesce_default : coalesce(colonne, defaut)
    - constant : constante littérale
    - flag_equality : colonne == valeur ? 1 : 0

    Non supporté (intentionnellement) :
    - TOUTE expression nécessitant eval()
    - TOUTE arithmétique entre colonnes
    - TOUTE expression logique multi-colonnes
    - TOUTE opération sur les dates

    La logique complexe doit être implémentée dans le code PySpark,
    pas dans la configuration. Cette fonction reste volontairement minimale.
    """

    ctype = cfg.get("type", "").lower()

    # -----------------------------------
    # coalesce_default
    # -----------------------------------
    if ctype == "coalesce_default":
        src = cfg["source_col"].lower()
        default = cfg.get("default")
        if src not in df.columns:
            return lit(default)
        return coalesce(col(src), lit(default))

    # -----------------------------------
    # constant
    # -----------------------------------
    if ctype == "constant":
        return lit(cfg.get("value"))

    # -----------------------------------
    # flag equality
    # -----------------------------------
    if ctype == "flag_equality":
        src = cfg["source_col"].lower()
        val = cfg["value"]
        if src not in df.columns:
            return lit(0)
        return (col(src) == val).cast("int")

    # -----------------------------------
    # Non supporté -> lever une erreur
    # -----------------------------------
    raise ValueError(
        f"Type de colonne calculée non supporté '{ctype}'. "
        f"Les expressions complexes doivent être implémentées dans le code PySpark."
    )
