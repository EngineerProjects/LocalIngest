# -*- coding: utf-8 -*-
"""
===============================================================================
LECTEUR DE DONNÉES BRONZE - Pipeline Construction
===============================================================================

Ce module permet de lire les fichiers de données bruts depuis la couche Bronze
(première couche de stockage) et de les préparer pour l'analyse.

PRINCIPE DE FONCTIONNEMENT :
---------------------------
1. Lit les fichiers CSV ou Parquet depuis le datalake Azure
2. Normalise les noms de colonnes (tout en minuscules)
3. Nettoie les valeurs NULL héritées de l'ancien système SAS
4. Convertit les colonnes vers les bons types de données (dates, nombres, etc.)
5. Applique des filtres pour ne garder que les données pertinentes
6. Fusionne tous les fichiers en un seul DataFrame

POURQUOI CET ORDRE D'OPÉRATIONS ?
----------------------------------
On nettoie AVANT de convertir les types car les marqueurs NULL de l'ancien
système (comme "." ou des espaces) doivent d'abord être transformés en vrais
NULL que Spark comprend. Sinon, la conversion des types échouerait.

On applique les filtres APRÈS le nettoyage pour éviter de supprimer par erreur
des lignes qui auraient des valeurs NULL mal formatées.
"""

import json
import os
import fnmatch
from pathlib import Path
from typing import List, Dict, Optional, Any

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, input_file_name, lit, trim, regexp_replace, when, to_date
)
from pyspark.sql.types import StringType, DateType

from utils.loaders.config_loader import ConfigLoader
from utils.transformations import lowercase_all_columns


class BronzeReader:
    """
    =======================================================================
    LECTEUR DE FICHIERS BRONZE
    =======================================================================
    
    Cette classe lit les fichiers de données bruts depuis la couche Bronze
    et les prépare pour les transformations métier.
    
    QUE FAIT CETTE CLASSE ?
    -----------------------
    - Lit des groupes de fichiers CSV ou Parquet depuis Azure Datalake
    - Met tous les noms de colonnes en minuscules pour éviter les erreurs
    - Nettoie les valeurs NULL mal formatées (héritées de l'ancien système)
    - Convertit les colonnes vers les bons types (dates, entiers, etc.)
    - Applique des filtres pour ne garder que certaines données
    - Fusionne tous les fichiers en un seul tableau de données
    
    EXEMPLE D'UTILISATION :
    ----------------------
    >>> reader = BronzeReader(spark, config, logger=logger)
    >>> df = reader.read_file_group('ipf', '202509')
    >>> # Lit les fichiers IPF pour septembre 2025
    
    PARAMÈTRES D'INITIALISATION :
    ----------------------------
    - spark : La session Spark active (moteur de traitement de données)
    - config : L'objet de configuration du pipeline
    - reading_config_path : Chemin vers le fichier de configuration (optionnel)
    - logger : Système de journalisation pour suivre les opérations (optionnel)
    """

    def __init__(
        self,
        spark: SparkSession,
        config: ConfigLoader,
        reading_config_path: Optional[str] = None,
        logger=None
    ):
        """
        Initialise le lecteur de fichiers Bronze.
        
        Charge la configuration qui décrit quels fichiers lire et comment
        les interpréter (séparateurs, encodage, schémas de colonnes, etc.).
        """
        self.spark = spark
        self.config = config
        self.logger = logger

        # Déterminer où se trouve le fichier de configuration de lecture
        if reading_config_path is None:
            # Par défaut : chercher dans le répertoire config/ du projet
            project_root = Path(__file__).parent.parent
            reading_config_path = project_root / "config" / "reading_config.json"

        # Charger le fichier JSON qui décrit tous les groupes de fichiers à lire
        with open(reading_config_path, 'r', encoding='utf-8') as f:
            self.reading_config = json.load(f)

        # Récupérer la fonction qui fournit les schémas de colonnes
        # (définit les types de données attendus pour chaque fichier)
        from config.column_definitions import get_schema
        self.get_schema = get_schema

    # =========================================================================
    # MÉTHODES PUBLIQUES (utilisables depuis l'extérieur)
    # =========================================================================

    def read_file_group(
        self,
        file_group: str,
        vision: str,
        custom_filters: Optional[List[Dict[str, Any]]] = None
    ) -> DataFrame:
        """
        Lit un groupe de fichiers depuis la couche Bronze et retourne un DataFrame nettoyé.
        
        PRINCIPE DE FONCTIONNEMENT :
        ---------------------------
        1. Charge la configuration du groupe de fichiers demandé
        2. Construit le chemin vers les fichiers sur le datalake Azure
        3. Lit chaque fichier individuellement (fragments)
        4. Pour chaque fragment :
           - Normalise les colonnes en minuscules
           - Ajoute des colonnes dynamiques si configuré
           - Nettoie les NULL legacy (ancien système)
           - Convertit vers les bons types de données
           - Applique les filtres
        5. Fusionne tous les fragments en un seul DataFrame
        
        PARAMÈTRES :
        -----------
        file_group : str
            Nom du groupe de fichiers à lire (ex: 'ipf', 'capitxcu', etc.)
            Défini dans reading_config.json
            
        vision : str
            Période à traiter au format YYYYMM (ex: '202509' pour septembre 2025)
            
        custom_filters : list[dict], optionnel
            Filtres supplémentaires à appliquer sur les données
            
        RETOUR :
        -------
        DataFrame
            Tableau de données Spark nettoyé et filtré, prêt pour les transformations
            
        ERREURS POSSIBLES :
        ------------------
        - ValueError : si le groupe de fichiers n'existe pas dans la configuration
        - FileNotFoundError : si aucun fichier correspondant n'est trouvé
        """

        # Vérifier que le groupe de fichiers existe dans la configuration
        if file_group not in self.reading_config['file_groups']:
            raise ValueError(f"Le groupe de fichiers '{file_group}' n'existe pas dans reading_config.json")

        # Récupérer la configuration spécifique à ce groupe de fichiers
        group_cfg = self.reading_config['file_groups'][file_group]

        # =====================================================================
        # ÉTAPE 1 : Construire le chemin vers les fichiers sur Azure
        # =====================================================================
        
        # Récupérer le conteneur Azure (possibilité de surcharge par variable d'environnement)
        container = os.getenv('DATALAKE_CONTAINER') or self.config.get('environment.container')
        data_root = self.config.get('datalake.data_root')
        
        # Déterminer si les fichiers sont mensuels ou de référence
        location_type = group_cfg.get('location_type', 'monthly')
        
        if location_type == 'reference':
            # Fichiers de référence : ne changent pas chaque mois (ex: tables de mapping)
            path_template = self.config.get('datalake.paths.bronze_reference')
            bronze_path = path_template.format(
                container=container,
                data_root=data_root
            )
        else:
            # Fichiers mensuels : un nouveau fichier chaque mois
            path_template = self.config.get('datalake.paths.bronze_monthly')
            
            # Extraire l'année et le mois depuis la vision (ex: '202509' → année=2025, mois=9)
            from utils.helpers import extract_year_month_int
            year, month = extract_year_month_int(vision)
            
            bronze_path = path_template.format(
                container=container,
                data_root=data_root,
                year=year,
                month=f"{month:02d}"  # Format avec zéro devant si nécessaire (ex: 09)
            )

        # =====================================================================
        # ÉTAPE 2 : Préparer les patterns de fichiers à lire
        # =====================================================================
        
        file_patterns: List[str] = group_cfg['file_patterns']
        
        # Remplacer le placeholder {vision} dans les noms de fichiers
        # Ex: "ird_risk_q45_{vision}.csv" devient "ird_risk_q45_202509.csv"
        file_patterns = [pattern.replace('{vision}', vision) for pattern in file_patterns]
        
        # Récupérer les autres paramètres de configuration
        schema_name: Optional[str] = group_cfg.get('schema')
        filtres: List[Dict[str, Any]] = group_cfg.get('filtres', [])
        read_options: Dict[str, Any] = group_cfg.get('read_options', {})
        dynamic_columns: List[Dict[str, Any]] = group_cfg.get('dynamic_columns', [])

        # Charger le schéma de colonnes si spécifié
        # Le schéma définit les types de données attendus (texte, nombre, date, etc.)
        schema = self.get_schema(schema_name) if schema_name else None

        # =====================================================================
        # ÉTAPE 3 : Lire tous les fichiers fragment par fragment
        # =====================================================================
        
        fragments: List[DataFrame] = []

        for pattern in file_patterns:
            # Construire le chemin complet vers le fichier
            full_pattern = f"{bronze_path}/{pattern}"
            
            try:
                # Lire le fichier (CSV ou Parquet)
                df_part = self._read_by_format(
                    full_pattern,
                    pattern,
                    schema,
                    read_options,
                    schema_name=schema_name
                )

                # Ajouter une colonne qui indique d'où vient chaque ligne
                # Utile pour tracer l'origine des données en cas de problème
                df_part = df_part.withColumn("_source_file", input_file_name())

                # Ajouter des colonnes dynamiques si configuré
                # Ex: ajouter "cdpole=1" pour les fichiers IPF16, "cdpole=3" pour IPF36
                if dynamic_columns:
                    df_part = self._apply_dynamic_columns(df_part, dynamic_columns)

                # S'assurer que tous les noms de colonnes sont en minuscules
                df_part = lowercase_all_columns(df_part)

                # ---------------------------------------------------------
                # NETTOYAGE ET TRANSFORMATION DE CE FRAGMENT
                # ---------------------------------------------------------
                
                # Nettoyer les marqueurs NULL de l'ancien système SAS
                # Transforme ".", "" et espaces en vrais NULL Spark
                df_part = self._clean_sas_nulls(df_part)

                # Convertir les colonnes vers les bons types de données
                if schema:
                    # Récupérer le format de date depuis la configuration
                    date_format = read_options.get('dateFormat', 'dd/MM/yyyy')
                    df_part = self._safe_cast(df_part, schema, date_format)

                # Appliquer les filtres configurés dans reading_config.json
                if filtres:
                    df_part = self._apply_read_filters(df_part, filtres)

                # Appliquer les filtres supplémentaires passés en paramètre
                if custom_filters:
                    df_part = self._apply_read_filters(df_part, custom_filters)

                # Ajouter ce fragment à la liste des fragments traités
                fragments.append(df_part)

            except Exception as e:
                # Si un fichier ne peut pas être lu, on le signale et on continue
                if self.logger:
                    self.logger.debug(f"[BronzeReader] Pattern {pattern} ignoré : {e}")
                continue

        # Vérifier qu'au moins un fichier a été trouvé
        if not fragments:
            raise FileNotFoundError(
                f"Aucun fichier trouvé pour les patterns {file_patterns} dans {bronze_path}"
            )

        # =====================================================================
        # ÉTAPE 4 : Fusionner tous les fragments en un seul DataFrame
        # =====================================================================
        
        # Commencer avec le premier fragment
        df = fragments[0]
        
        # Ajouter tous les autres fragments ligne par ligne
        for nxt in fragments[1:]:
            # unionByName : fusionne par nom de colonne (pas par position)
            # allowMissingColumns : autorise des colonnes manquantes dans certains fragments
            df = df.unionByName(nxt, allowMissingColumns=True)

        # Retourner le DataFrame final (nettoyé, casté, filtré, fusionné)
        return df


    def list_available_file_groups(self) -> List[str]:
        """
        Liste tous les groupes de fichiers disponibles dans la configuration.
        
        UTILITÉ :
        --------
        Permet de savoir quels sont les groupes de fichiers qu'on peut lire
        sans avoir à ouvrir le fichier de configuration manuellement.
        
        RETOUR :
        -------
        list[str]
            Liste des noms de groupes de fichiers (ex: ['ipf', 'capitxcu', 'constrcu'])
        """
        return list(self.reading_config['file_groups'].keys())

    # =========================================================================
    # MÉTHODES INTERNES (utilisées uniquement à l'intérieur de cette classe)
    # =========================================================================

    def _read_by_format(
        self,
        path: str,
        pattern: str,
        schema: Optional[Any],
        read_options: Dict[str, Any],
        schema_name: str = None
    ) -> DataFrame:
        """
        Lit un fichier en détectant automatiquement son format (CSV ou Parquet).
        
        DÉTECTION AUTOMATIQUE DU FORMAT :
        ---------------------------------
        - Si le pattern se termine par .csv → format CSV
        - Sinon (pas d'extension) → format Parquet (dossier partitionné)
        
        DIFFÉRENCES DE TRAITEMENT :
        --------------------------
        CSV : 
            - Lu sans inférence de types (tout en texte au départ)
            - Utilise les options de lecture (séparateur, encodage, etc.)
            
        Parquet : 
            - Lu avec les types natifs déjà définis dans le fichier
            - Les options CSV sont ignorées
        
        PARAMÈTRES :
        -----------
        path : str
            Chemin complet vers le fichier ou pattern avec wildcards
            
        pattern : str
            Pattern de fichier utilisé pour détecter le format
            
        schema : StructType, optionnel
            Schéma cible pour sélectionner les colonnes à garder
            
        read_options : dict
            Options de lecture pour les fichiers CSV (séparateur, encodage, etc.)
            
        RETOUR :
        -------
        DataFrame
            Données du fichier avec colonnes en minuscules et projection si schéma fourni
        """
        
        # Détecter le format du fichier depuis le pattern
        if pattern.endswith('.csv'):
            file_format = 'csv'
        elif '.' not in pattern or pattern.endswith('/'):
            file_format = 'parquet'
        else:
            raise ValueError(
                f"Format de fichier non supporté : {pattern}. "
                f"Attendu : .csv (CSV) ou sans extension (Parquet)"
            )
        
        # Retirer la clé 'format' des options si présente (ancienne version)
        spark_options = {k: v for k, v in read_options.items() if k != 'format'}

        # Lire le fichier selon son format
        if file_format == 'csv':
            # CSV : lire tout en texte (inferSchema=False)
            # On convertira vers les bons types plus tard avec _safe_cast
            df_all = (
                self.spark.read
                    .options(**spark_options)
                    .csv(path, header=True, inferSchema=False)
            )
        elif file_format == 'parquet':
            # Parquet : lire avec les types natifs déjà présents dans le fichier
            # Ignorer les options CSV qui ne s'appliquent pas
            df_all = self.spark.read.parquet(path)
        else:
            raise ValueError(f"Format non supporté : {file_format}")

        # Normaliser tous les noms de colonnes en minuscules
        # Évite les erreurs dues à la casse (ex: "CDPOLE" vs "cdpole")
        df_all = lowercase_all_columns(df_all)

        # Si un schéma est fourni, ne garder que les colonnes qui nous intéressent
        if schema:
            # Liste des colonnes attendues (en minuscules)
            wanted = [f.name.lower() for f in schema.fields]
            
            # Garder uniquement les colonnes qui existent ET qui sont voulues
            available = [c for c in df_all.columns if c in wanted]
            
            # Retourner le DataFrame avec seulement ces colonnes
            # Si aucune colonne trouvée, retourner un DataFrame vide
            return df_all.select(*available) if available else df_all.limit(0)

        return df_all

    def _apply_dynamic_columns(self, df: DataFrame, rules: List[Dict[str, Any]]) -> DataFrame:
        """
        Ajoute des colonnes avec des valeurs qui dépendent du nom du fichier source.
        
        POURQUOI CETTE FONCTION ?
        -------------------------
        Certaines informations ne sont pas dans les fichiers mais sont déduites
        du nom du fichier. Par exemple :
        - Les fichiers IPF16.csv concernent le pôle 1 (agents)
        - Les fichiers IPF36.csv concernent le pôle 3 (courtiers)
        
        On doit donc ajouter une colonne "cdpole" avec la bonne valeur selon
        le fichier dont provient chaque ligne.
        
        CONFIGURATION EXEMPLE :
        ----------------------
        "dynamic_columns": [
          {"pattern": "*16*", "columns": {"cdpole": "1"}},
          {"pattern": "*36*", "columns": {"cdpole": "3"}}
        ]
        
        COMMENT ÇA MARCHE ?
        ------------------
        1. Pour chaque règle de configuration :
           - On vérifie si le nom du fichier correspond au pattern (ex: "*16*")
           - Si oui, on ajoute ou modifie les colonnes spécifiées
           
        2. Le traitement est vectorisé : toutes les lignes d'un même fichier
           reçoivent la même valeur en une seule opération (rapide)
        
        PARAMÈTRES :
        -----------
        df : DataFrame
            Données avec une colonne '_source_file' contenant le nom de fichier
            
        rules : list[dict]
            Liste de règles avec 'pattern' (motif) et 'columns' (colonnes à ajouter)
            
        RETOUR :
        -------
        DataFrame
            Données enrichies avec les nouvelles colonnes
        """
        
        # Vérifier que la colonne _source_file existe et qu'il y a des règles
        if "_source_file" not in df.columns or not rules:
            return df

        # Traiter chaque règle une par une
        for rule in rules:
            pattern = rule.get("pattern", "")
            cols_to_add: Dict[str, Any] = rule.get("columns", {})
            
            # Convertir le pattern glob (ex: "*16*") en expression régulière
            regex = fnmatch.translate(pattern)
            
            # Retirer le marqueur de fin \Z pour permettre les correspondances partielles
            if regex.endswith(r"\Z"):
                regex = regex[:-2]

            # Créer une condition : la colonne _source_file correspond au pattern ?
            cond = col("_source_file").rlike(regex)
            
            # Pour chaque colonne à ajouter
            for k, v in cols_to_add.items():
                k_low = k.lower()  # Forcer le nom en minuscules
                
                if k_low in df.columns:
                    # La colonne existe déjà : la modifier seulement si le pattern correspond
                    df = df.withColumn(k_low, when(cond, lit(v)).otherwise(col(k_low)))
                else:
                    # La colonne n'existe pas : la créer avec la valeur si le pattern correspond
                    df = df.withColumn(k_low, when(cond, lit(v)))

        # S'assurer que tous les noms de colonnes sont en minuscules après ajouts
        df = lowercase_all_columns(df)
        return df

    def _clean_sas_nulls(self, df: DataFrame) -> DataFrame:
        """
        Nettoie les marqueurs NULL non standard présents dans les fichiers CSV sources.
        
        PROBLÈME À RÉSOUDRE :
        --------------------
        Certains fichiers CSV sources utilisent des conventions spéciales pour marquer
        les valeurs manquantes :
        - Un point "." pour représenter NULL
        - Des chaînes vides ""
        - Des espaces insécables (caractère spécial non visible)
        
        Spark ne reconnaît pas ces valeurs comme des NULL, ce qui cause des
        problèmes lors des comparaisons et des conversions de types.
        
        SOLUTION APPLIQUÉE :
        -------------------
        1. Remplacer les espaces insécables (NBSP) par des espaces normaux
        2. Enlever les espaces au début et à la fin des textes (trim)
        3. Transformer "." en vrai NULL Spark
        4. Transformer "" (chaîne vide) en vrai NULL Spark
        
        IMPORTANT :
        ----------
        On ne nettoie QUE les colonnes de type texte (string).
        Les colonnes numériques ou dates ne sont pas concernées.
        
        PARAMÈTRES :
        -----------
        df : DataFrame
            Données à nettoyer (peut contenir différents types de colonnes)
            
        RETOUR :
        -------
        DataFrame
            Mêmes données avec les colonnes texte nettoyées et vrais NULL Spark
        """
        
        # Identifier les colonnes de type texte uniquement
        string_cols = [c for c, t in df.dtypes if t == "string"]
        
        # Si aucune colonne texte, rien à faire
        if not string_cols:
            return df

        # Construire une liste d'expressions pour transformer chaque colonne
        select_exprs = []
        
        for c in df.columns:
            if c in string_cols:
                # Pour les colonnes texte : appliquer le nettoyage
                
                # Étape 1 : Remplacer NBSP par espace normal et enlever espaces aux extrémités
                tmp = trim(regexp_replace(col(c), u"\u00A0", " "))
                
                # Étape 2 : Si la valeur est "." ou "", la remplacer par NULL
                expr = when((tmp == ".") | (tmp == ""), lit(None).cast(StringType())) \
                       .otherwise(tmp) \
                       .alias(c)
                       
                select_exprs.append(expr)
            else:
                # Pour les autres colonnes : les garder telles quelles
                select_exprs.append(col(c))

        # Appliquer toutes les transformations en une seule opération (performant)
        return df.select(*select_exprs)


    def _safe_cast(self, df: DataFrame, schema, date_format: str = "dd/MM/yyyy"):
        """
        Convertit les colonnes vers les types de données définis dans le schéma.
        
        POURQUOI CETTE FONCTION ?
        -------------------------
        Les fichiers CSV sont lus avec toutes les colonnes en texte au départ.
        Il faut ensuite convertir chaque colonne vers son vrai type :
        - Les dates vers le type Date
        - Les nombres vers Integer ou Double
        - Etc.
        
        TRAITEMENT SPÉCIAL DES DATES :
        -----------------------------
        Les dates peuvent être dans différents formats selon les fichiers :
        - Format européen : "31/12/2025" (jour/mois/année)
        - Format ISO : "2025-12-31" (année-mois-jour)
        
        On utilise le format spécifié dans la configuration pour chaque groupe
        de fichiers (dateFormat dans read_options).
        
        SÉCURITÉ :
        ---------
        Si une conversion échoue (ex: texte qui ne ressemble pas à un nombre),
        Spark met NULL au lieu de planter. Cela permet de continuer le traitement
        et d'identifier les problèmes de qualité de données plus tard.
        
        PARAMÈTRES :
        -----------
        df : DataFrame
            Données nettoyées (après _clean_sas_nulls)
            
        schema : StructType
            Schéma définissant le nom et le type attendu de chaque colonne
            
        date_format : str
            Format de date à utiliser pour parser les colonnes date
            Par défaut : 'dd/MM/yyyy' (format européen)
            
        RETOUR :
        -------
        DataFrame
            Données avec les colonnes converties vers les bons types
        """
        
        # Créer un dictionnaire : nom_colonne → type_attendu
        cast_map = {f.name.lower(): f.dataType for f in schema.fields}

        # Construire une expression de sélection pour chaque colonne
        select_exprs = []
        
        for c in df.columns:
            # Récupérer le type attendu pour cette colonne
            dtype = cast_map.get(c)
            
            if dtype is None:
                # Pas de type défini : garder la colonne telle quelle
                select_exprs.append(col(c))
                
            elif isinstance(dtype, DateType):
                # Type Date : utiliser to_date avec le format spécifié
                # Ex: "31/12/2025" → Date(2025, 12, 31)
                select_exprs.append(to_date(col(c), date_format).alias(c))
                
            else:
                # Autres types (Integer, Double, etc.) : conversion standard
                select_exprs.append(col(c).cast(dtype).alias(c))

        # Appliquer toutes les conversions en une seule opération
        return df.select(*select_exprs)

    def _apply_read_filters(
        self,
        df: DataFrame,
        filters: List[Dict[str, Any]]
    ) -> DataFrame:
        """
        Applique des filtres pour ne garder que certaines lignes de données.
        
        POURQUOI FILTRER ?
        -----------------
        Les fichiers sources contiennent souvent plus de données que nécessaire.
        Par exemple, un fichier peut contenir tous les marchés (Construction,
        Santé, Auto, etc.) mais on ne veut garder que Construction.
        
        Les filtres permettent de :
        - Réduire la taille des données à traiter (plus rapide)
        - Garder seulement les données pertinentes pour l'analyse
        
        GESTION SPÉCIALE DES NULL :
        --------------------------
        Utilise une sémantique spéciale pour les NULL :
        - equals (==):     col == value           → NULL exclus (standard PySpark)
        - not_equals (!=): col IS NULL OR col != value  → NULL INCLUS (comportement spécial !)
        - in:              col IS NOT NULL AND col IN (...)  → NULL exclus (standard PySpark)
        - not_in:          col IS NULL OR col NOT IN (...)   → NULL INCLUS (comportement spécial !)
        - comparisons (>, <, >=, <=) : exclut les NULL (Spark standard)
        
        OPÉRATEURS SUPPORTÉS :
        ---------------------
        - '==' : égal à
        - '!=' : différent de
        - 'in' : présent dans une liste
        - 'not_in' : absent d'une liste
        - '>', '<', '>=', '<=' : comparaisons numériques
        
        EXEMPLE DE FILTRE :
        ------------------
        {
          "column": "cmarch",
          "operator": "==",
          "value": "6"
        }
        → Ne garde que les lignes où cmarch vaut "6" (marché Construction)
        
        PARAMÈTRES :
        -----------
        df : DataFrame
            Données à filtrer
            
        filters : list[dict]
            Liste de filtres à appliquer, chaque filtre contient :
            - column : nom de la colonne
            - operator : opérateur de comparaison
            - value : valeur de référence
            
        RETOUR :
        -------
        DataFrame
            Données filtrées (moins de lignes si filtres actifs)
        """
        
        # Appliquer chaque filtre un par un
        for f in filters:
            colname = f.get('column', '').lower()  # Nom de colonne en minuscules
            operator = f.get('operator')
            value = f.get('value')

            # Ignorer le filtre si des informations manquent
            if not colname or colname not in df.columns or not operator:
                continue

            # Opérateur EQUALS (==) : garde les lignes où colonne = valeur
            # Les NULL sont exclus automatiquement (comportement Spark standard)
            if operator == "==":
                df = df.filter(col(colname) == value)
            
            # Opérateur NOT EQUALS (!=) : garde les lignes où colonne ≠ valeur
            # SPÉCIAL : inclut aussi les NULL (pour compatibilité avec les fichiers sources)
            elif operator == "!=":
                df = df.filter(col(colname).isNull() | (col(colname) != value))
            
            # Opérateur IN : garde les lignes où colonne fait partie de la liste
            # Les NULL sont exclus (standard)
            elif operator == "in":
                vals = value if isinstance(value, list) else [value]
                df = df.filter(col(colname).isNotNull() & col(colname).isin(vals))
            
            # Opérateur NOT IN : garde les lignes où colonne ne fait pas partie de la liste
            # SPÉCIAL : inclut aussi les NULL (pour compatibilité avec les fichiers sources)
            elif operator == "not_in":
                vals = value if isinstance(value, list) else [value]
                df = df.filter(col(colname).isNull() | (~col(colname).isin(vals)))
            
            # Opérateurs de comparaison numériques
            # Les NULL sont exclus dans tous les cas (standard)
            elif operator == ">":
                df = df.filter(col(colname) > value)
            elif operator == "<":
                df = df.filter(col(colname) < value)
            elif operator == ">=":
                df = df.filter(col(colname) >= value)
            elif operator == "<=":
                df = df.filter(col(colname) <= value)

        return df


class SilverReader:
    """
    =======================================================================
    LECTEUR DE FICHIERS SILVER
    =======================================================================
    
    Cette classe lit les fichiers de la couche Silver (deuxième couche de
    stockage), qui contient des données déjà nettoyées et structurées.
    
    DIFFÉRENCE AVEC BronzeReader :
    ------------------------------
    - BronzeReader lit les fichiers bruts (CSV avec problèmes de qualité)
    - SilverReader lit les fichiers propres (Parquet ou Delta, déjà typés)
    
    Le SilverReader est plus simple car les données sont déjà nettoyées.
    
    FORMATS SUPPORTÉS :
    ------------------
    - Delta Lake (recommandé, permet des mises à jour ACID)
    - Parquet (format colonne performant)
    - CSV (pour compatibilité uniquement)
    
    Le format est configuré dans config.yml (clé output.format).
    """

    def __init__(
        self,
        spark: SparkSession,
        config: ConfigLoader
    ):
        """
        Initialise le lecteur de fichiers Silver.
        
        PARAMÈTRES :
        -----------
        spark : SparkSession
            Session Spark active (moteur de traitement)
            
        config : ConfigLoader
            Objet de configuration du pipeline
        """
        self.spark = spark
        self.config = config

    def read_silver_file(
        self,
        filename: str,
        vision: str
    ) -> DataFrame:
        """
        Lit un fichier depuis la couche Silver en détectant automatiquement le format.
        
        FONCTIONNEMENT :
        ---------------
        1. Construit le chemin vers le fichier sur Azure Datalake
        2. Détecte le format configuré (Delta, Parquet ou CSV)
        3. Lit le fichier avec le bon lecteur Spark
        4. S'assure que les colonnes sont en minuscules
        
        AVANTAGE DE LA DÉTECTION AUTOMATIQUE :
        --------------------------------------
        On peut changer le format de stockage (ex: passer de Parquet à Delta)
        en modifiant juste la configuration, sans toucher au code du pipeline.
        
        PARAMÈTRES :
        -----------
        filename : str
            Nom du fichier sans extension (ex: 'mvt_const_ptf')
            
        vision : str
            Période au format YYYYMM (ex: '202509')
            
        RETOUR :
        -------
        DataFrame
            Données du fichier Silver (propres, typées, colonnes en minuscules)
            
        EXEMPLE :
        --------
        >>> reader = SilverReader(spark, config)
        >>> df = reader.read_silver_file('mvt_const_ptf', '202509')
        >>> # Lit les mouvements de portefeuille pour septembre 2025
        """
        
        # Construire le chemin vers le répertoire Silver
        container = os.getenv('DATALAKE_CONTAINER') or self.config.get('environment.container')
        data_root = self.config.get('datalake.data_root')
        path_template = self.config.get('datalake.paths.silver')
        
        # Extraire année et mois depuis la vision
        from utils.helpers import extract_year_month_int
        year, month = extract_year_month_int(vision)
        
        silver_path = path_template.format(
            container=container,
            data_root=data_root,
            year=year,
            month=f"{month:02d}"
        )
        
        # Récupérer le format configuré (par défaut Parquet pour compatibilité)
        output_format = self.config.get('output.format', 'parquet').lower()
        
        # Lire selon le format
        if output_format == "delta":
            # Delta Lake : le fichier est en fait un répertoire avec métadonnées
            full_path = f"{silver_path}/{filename}"
            df = self.spark.read.format("delta").load(full_path)
            
        elif output_format == "parquet":
            # Parquet : fichier unique avec extension .parquet
            full_path = f"{silver_path}/{filename}.parquet"
            df = self.spark.read.parquet(full_path)
            
        elif output_format == "csv":
            # CSV : fichier texte avec séparateur point-virgule
            full_path = f"{silver_path}/{filename}.csv"
            df = self.spark.read.option("header", True).option("delimiter", ";").csv(full_path)
            
        else:
            raise ValueError(f"Format Silver non supporté : {output_format}")
        
        # S'assurer que les colonnes sont en minuscules
        # (normalement déjà fait lors de l'écriture Silver, mais on vérifie)
        from utils.transformations import lowercase_all_columns
        df = lowercase_all_columns(df)
        
        return df

    def read_multiple_silver_files(
        self,
        filenames: List[str],
        vision: str
    ) -> Dict[str, DataFrame]:
        """
        Lit plusieurs fichiers Silver en une seule fois.
        
        UTILITÉ :
        --------
        Quand on a besoin de plusieurs fichiers pour une transformation,
        cette fonction permet de tous les charger d'un coup et de les
        récupérer dans un dictionnaire pratique.
        
        PARAMÈTRES :
        -----------
        filenames : list[str]
            Liste des noms de fichiers à lire (sans extension)
            
        vision : str
            Période au format YYYYMM
            
        RETOUR :
        -------
        dict[str, DataFrame]
            Dictionnaire où la clé est le nom du fichier et la valeur
            est le DataFrame correspondant
            
        EXEMPLE :
        --------
        >>> reader = SilverReader(spark, config)
        >>> dfs = reader.read_multiple_silver_files(
        ...     ['mvt_const_ptf', 'azec_ptf'],
        ...     '202509'
        ... )
        >>> df_az = dfs['mvt_const_ptf']
        >>> df_azec = dfs['azec_ptf']
        """
        result = {}
        for filename in filenames:
            result[filename] = self.read_silver_file(filename, vision)
        
        return result
