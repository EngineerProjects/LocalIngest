"""
Assistants pour les processeurs - Données de référence consolidées & Utilitaires de déduplication.

Fournit des modèles réutilisables pour :
1. Jointures sûres de données de référence avec repli automatique sur NULL
2. Ajout en masse de colonnes NULL
3. Enrichissement de la segmentation
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, broadcast, col
from pyspark.sql.types import StringType, DoubleType, IntegerType, DateType
from typing import Union, List, Dict, Optional


# ============================================================================
# Fabrique BronzeReader
# ============================================================================

def get_bronze_reader(processor):
    """
    Fonction fabrique pour créer une instance BronzeReader à partir du contexte du processeur.
    
    Élimine le motif répété : reader = BronzeReader(self.spark, self.config)
    
    Args:
        processor: Instance de processeur (sous-classe de BaseProcessor)
    
    Returns:
        Instance BronzeReader initialisée avec le spark et la config du processeur
    
    Exemple:
        reader = get_bronze_reader(self)
        df = reader.read_file_group('ipf', vision)
    """
    from src.reader import BronzeReader
    return BronzeReader(processor.spark, processor.config)


# ============================================================================
# Assistants de Jointure de Données de Référence
# ============================================================================

def safe_reference_join(
    df: DataFrame,
    reader,
    file_group: str,
    vision: Union[str, None],
    join_keys: Union[str, List[str]],
    select_columns: List[str],
    null_columns: Optional[Dict[str, type]] = None,
    filter_condition: Optional[str] = None,
    use_broadcast: bool = True,
    how: str = "left",
    logger = None,
    required: bool = False
) -> DataFrame:
    """
    Jointure sûre de données de référence avec gestion d'erreurs configurable.
    
    Args:
        df: DataFrame en entrée
        reader: Instance BronzeReader
        file_group: Nom du groupe de fichiers à lire
        vision: Chaîne vision ou 'ref' pour les données de référence
        join_keys: Nom(s) de colonne(s) pour la jointure
        select_columns: Colonnes à sélectionner de la table de référence
        null_columns: Dict de {colonne: type} pour le repli NULL (si None, déduit de select_columns)
        filter_condition: Filtre optionnel à appliquer sur la table de référence (ex: "cmarch == '6'")
        use_broadcast: Si True, broadcast la table de référence
        how: Type de jointure (défaut : 'left')
        logger: Instance logger pour les messages info/warning
        required: Si True, lève une erreur quand les données de référence manquent (fail-fast).
                  Si False, ajoute des colonnes NULL en repli (défaut : False)
    
    Returns:
        DataFrame avec les colonnes de référence jointes ou colonnes NULL si indisponible (quand required=False)
    
    Raises:
        RuntimeError: Quand required=True et que les données de référence sont indisponibles
    """
    if logger:
        logger.debug(f"Tentative de jointure des données de référence : {file_group} (required={required})")
    
    try:
        # Lire les données de référence
        df_ref = reader.read_file_group(file_group, vision)
        
        if df_ref is None:
            if required:
                error_msg = f"Les données de référence requises '{file_group}' sont indisponibles (retourné None)"
                if logger:
                    logger.error(error_msg)
                raise RuntimeError(error_msg)
            else:
                if logger:
                    logger.warning(f"Les données de référence optionnelles '{file_group}' ont retourné None - ajout de colonnes NULL")
                return _add_null_columns(df, null_columns or _infer_null_columns(select_columns))
        
        # Appliquer le filtre si fourni
        if filter_condition:
            df_ref = df_ref.filter(filter_condition)
        
        # Normaliser les clés de jointure en liste
        if isinstance(join_keys, str):
            join_keys = [join_keys]
        
        # Sélectionner uniquement les colonnes nécessaires (+ clés de jointure)
        needed_cols = list(set(join_keys + select_columns))
        df_ref = df_ref.select(*[col(c) for c in needed_cols if c in df_ref.columns])
        
        # Appliquer broadcast si demandé
        if use_broadcast:
            df_ref = broadcast(df_ref)
        
        # Effectuer la jointure
        df_result = df.join(df_ref, on=join_keys, how=how)
        
        if logger:
            logger.info(f"Données de référence jointes avec succès : {file_group}")
        
        return df_result
        
    except FileNotFoundError as e:
        if required:
            error_msg = f"Fichier de référence requis '{file_group}' non trouvé"
            if logger:
                logger.error(f"{error_msg} : {e}")
            raise RuntimeError(error_msg) from e
        else:
            if logger:
                logger.warning(f"Référence optionnelle '{file_group}' non trouvée : {e} - ajout de colonnes NULL")
            return _add_null_columns(df, null_columns or _infer_null_columns(select_columns))
    
    except Exception as e:
        # Toujours lever une erreur sur les erreurs inattendues (pas seulement les fichiers manquants)
        error_msg = f"Erreur inattendue lors de la jointure des données de référence '{file_group}' : {e}"
        if logger:
            logger.error(error_msg)
        raise RuntimeError(error_msg) from e


def safe_multi_reference_join(
    df: DataFrame,
    reader,
    joins: List[Dict],
    logger=None
) -> DataFrame:
    """
    Effectue plusieurs jointures de référence sûres en séquence.
    
    Args:
        df: DataFrame en entrée
        reader: Instance BronzeReader
        joins: Liste des spécifications de jointure, chacune étant un dictionnaire avec :
            - file_group: str
            - vision: str
            - join_keys: str ou List[str]
            - select_columns: List[str]
            - null_columns: Optional[Dict[str, type]]
            - filter_condition: Optional[str]
            - use_broadcast: bool (défaut True)
        logger: Instance logger
    
    Returns:
        DataFrame avec toutes les jointures de référence appliquées
    """
    for join_spec in joins:
        df = safe_reference_join(
            df, reader,
            file_group=join_spec['file_group'],
            vision=join_spec.get('vision', 'ref'),
            join_keys=join_spec['join_keys'],
            select_columns=join_spec['select_columns'],
            null_columns=join_spec.get('null_columns'),
            filter_condition=join_spec.get('filter_condition'),
            use_broadcast=join_spec.get('use_broadcast', True),
            how=join_spec.get('how', 'left'),
            logger=logger
        )
    
    return df


# ============================================================================
# Assistants Colonnes NULL
# ============================================================================

def add_null_columns(df: DataFrame, column_specs: Dict[str, type]) -> DataFrame:
    """
    Ajoute plusieurs colonnes NULL en une seule fois (opération en masse).
    
    Consolide les motifs comme :
        df = df.withColumn('col1', lit(None).cast(StringType()))
        df = df.withColumn('col2', lit(None).cast(StringType()))
    
    En :
        df = add_null_columns(df, {
            'col1': StringType,
            'col2': StringType
        })
    
    Args:
        df: DataFrame en entrée
        column_specs: Dict mappant les noms de colonnes aux types PySpark
    
    Returns:
        DataFrame avec les colonnes NULL ajoutées
    """
    for col_name, col_type in column_specs.items():
        df = df.withColumn(col_name, lit(None).cast(col_type()))
    return df


# ============================================================================
# Assistant Segmentation
# ============================================================================

def enrich_segmentation(
    df: DataFrame,
    reader,
    vision: str,
    market_filter: str = None,  # Défaut à MARKET_CODE.MARKET si None
    join_key: str = "cdprod",
    include_cdpole: bool = False,
    logger=None
) -> DataFrame:
    """
    Enrichit le DataFrame avec les données de segmentation de SEGMENTPRDT.
    
    Args:
        df: DataFrame en entrée
        reader: Instance BronzeReader
        vision: Chaîne vision
        market_filter: Filtre code marché (défaut : '6' pour construction)
        join_key: Colonne sur laquelle faire la jointure (défaut : 'cdprod')
        include_cdpole: Si True, jointure sur cdprod ET cdpole (logique émissions)
        logger: Instance logger
    
    Returns:
        DataFrame avec les colonnes de segmentation (cmarch, cseg, cssseg) ou repli NULL
    """
    if logger:
        logger.debug("Enrichissement avec les données de segmentation (SEGMENTPRDT)")
    
    # Défaut au marché construction si non spécifié
    if market_filter is None:
        from config.constants import MARKET_CODE
        market_filter = MARKET_CODE.MARKET
    
    try:
        df_seg = reader.read_file_group('segmentprdt', vision)
        
        if df_seg is None:
            if logger:
                logger.warning("SEGMENTPRDT non disponible - utilisation de valeurs NULL")
            return _add_seg_null_columns(df)
        
        # Filtrer pour le marché construction
        df_seg = df_seg.filter(col("cmarch") == market_filter)
        
        # Renommer cprod en cdprod si nécessaire (le fichier segmentation utilise 'cprod')
        if 'cprod' in df_seg.columns and join_key == 'cdprod':
            df_seg = df_seg.withColumnRenamed('cprod', 'cdprod')
        
        # Préparer les colonnes de jointure selon la logique métier
        if include_cdpole:
            # Logique EMISSIONS : Jointure sur cdprod ET cdpole
            # Cela assure une segmentation correcte pour chaque combinaison produit+canal
            if 'cdpole' not in df.columns:
                if logger:
                    logger.error("Colonne cdpole requise pour la jointure segmentation émissions !")
                return _add_seg_null_columns(df)
            
            # Sélectionner les colonnes nécessaires incluant cdpole
            df_seg = df_seg.select(join_key, 'cdpole', 'cmarch', 'cseg', 'cssseg') \
                           .dropDuplicates([join_key, 'cdpole'])
            
            # Jointure sur cdprod ET cdpole
            df_result = df.join(
                broadcast(df_seg),
                on=[join_key, 'cdpole'],
                how="left"
            )
            
            if logger:
                logger.info("Enrichissement segmentation réussi (jointure sur cdprod + cdpole)")
        else:
            # Logique CAPITAUX : Jointure sur cdprod uniquement
            df_seg = df_seg.select(join_key, 'cmarch', 'cseg', 'cssseg') \
                           .dropDuplicates([join_key])
            
            # Jointure avec broadcast
            df_result = df.join(
                broadcast(df_seg),
                on=join_key,
                how="left"
            )
            
            if logger:
                logger.info("Enrichissement segmentation réussi (jointure sur cdprod uniquement)")
        
        return df_result
        
    except Exception as e:
        if logger:
            logger.warning(f"Échec de l'enrichissement segmentation : {e}. Utilisation de valeurs NULL.")
        return _add_seg_null_columns(df)



# ============================================================================
# Helpers Internes
# ============================================================================

def _add_null_columns(df: DataFrame, column_specs: Dict[str, type]) -> DataFrame:
    """Helper interne pour ajouter des colonnes NULL (identique à la version publique)."""
    return add_null_columns(df, column_specs)


def _infer_null_columns(select_columns: List[str]) -> Dict[str, type]:
    """
    Déduit les types de colonnes NULL (défaut à StringType).
    
    Ceci est un repli quand null_columns n'est pas fourni.
    Pour une meilleure sécurité de type, toujours fournir null_columns explicitement.
    """
    return {col_name: StringType for col_name in select_columns}


def _add_seg_null_columns(df: DataFrame) -> DataFrame:
    """Ajoute les colonnes de segmentation NULL comme repli."""
    return (df
            .withColumn('cmarch', lit(None).cast(StringType()))
            .withColumn('cseg', lit(None).cast(StringType()))
            .withColumn('cssseg', lit(None).cast(StringType())))
