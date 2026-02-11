"""
Utilitaires pour le Pipeline de Données Construction.
Validation de visions, construction de chemins, calcul de plages de dates.
"""

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from typing import List, Tuple, Dict, Optional
from pathlib import Path
from pyspark.sql import SparkSession
from urllib.parse import urlparse


def validate_vision(vision: str) -> bool:
    """
    Valide le format de la vision (YYYYMM).

    PARAMÈTRES :
    -----------
    vision : str
        Vision à valider

    RETOUR :
    -------
    bool
        True si valide, False sinon

    EXEMPLE :
    --------
    >>> validate_vision("202509")
    True
    """
    if not isinstance(vision, str) or len(vision) != 6:
        return False

    try:
        year = int(vision[:4])
        month = int(vision[4:6])

        # Validation basique
        if year < 2000 or year > 2100:
            return False
        if month < 1 or month > 12:
            return False

        return True
    except ValueError:
        return False


def extract_year_month(vision: str) -> Tuple[str, str]:
    """
    Extrait l'année et le mois d'une vision.

    PARAMÈTRES :
    -----------
    vision : str
        Vision au format YYYYMM

    RETOUR :
    -------
    Tuple[str, str]
        (Année, Mois) sous forme de chaînes de caractères

    EXEMPLE :
    --------
    >>> year, month = extract_year_month("202509")
    >>> print(year, month)
    '2025' '09'
    """
    if not validate_vision(vision):
        raise ValueError(
            f"Format de vision invalide : {vision}. Attendu : YYYYMM (ex: 202509)"
        )

    year = vision[:4]
    month = vision[4:6]

    return year, month


def extract_year_month_int(vision: str) -> Tuple[int, int]:
    """
    Extrait l'année et le mois d'une vision sous forme d'entiers.

    PARAMÈTRES :
    -----------
    vision : str
        Vision au format YYYYMM

    RETOUR :
    -------
    Tuple[int, int]
        (Année, Mois) sous forme d'entiers
    """
    year_str, month_str = extract_year_month(vision)
    return int(year_str), int(month_str)


def build_layer_path(
    config,
    layer: str,
    vision: str
) -> str:
    """
    Construit le chemin d'accès pour une couche donnée du Data Lake.
    Utilise la nouvelle structure de configuration (container + data_root).

    PARAMÈTRES :
    -----------
    config : ConfigLoader
        Configuration chargée contenant environment.container et datalake.data_root
    layer : str
        Nom de la couche (bronze, silver, gold)
    vision : str
        Vision au format YYYYMM

    RETOUR :
    -------
    str
        Chemin complet vers la couche

    EXEMPLE :
    --------
    >>> path = build_layer_path(config, 'bronze', '202509')
    >>> # Retourne : abfs://container@.../ABR/.../bronze/2025/09
    """
    import os

    # Récupérer le conteneur (avec possibilité de surcharge via variable d'env)
    container = os.getenv('DATALAKE_CONTAINER') or config.get('environment.container')
    data_root = config.get('datalake.data_root')

    # Extraire année et mois
    year, month = extract_year_month_int(vision)

    # Construire le chemin : {container}{data_root}/{layer}/{year}/{month}
    path = f"{container}{data_root}/{layer}/{year}/{month:02d}"

    return path



def build_log_filename(vision: str) -> str:
    """
    Construit le nom du fichier de log pour une vision donnée.

    PARAMÈTRES :
    -----------
    vision : str
        Vision au format YYYYMM

    RETOUR :
    -------
    str
        Nom du fichier de log (ex: 'pipeline_202509.log')
    """
    if not validate_vision(vision):
        raise ValueError(f"Format de vision invalide : {vision}. Attendu : YYYYMM.")

    return f"pipeline_{vision}.log"


def compute_date_ranges(vision: str) -> Dict[str, str]:
    """
    Calcule toutes les plages de dates requises pour le traitement PTF_MVT.
    Génère les dates utilisées pour les calculs de mouvements, d'expositions, etc.

    PARAMÈTRES :
    -----------
    vision : str
        Vision au format YYYYMM

    RETOUR :
    -------
    Dict[str, str]
        Dictionnaire contenant les dates clés :
        - 'dtfin' : Dernier jour du mois de vision (YYYY-MM-DD)
        - 'dtdeb_an' : Premier jour de l'année de vision (YYYY-01-01)
        - 'dtfinmm' : Dernier jour du mois de vision (Identique dtfin)
        - 'dtfinmm1' : Dernier jour du mois précédent
        - 'dtdebn' : Premier jour du mois de vision
        - 'dtfinmn' : Dernier jour du mois de vision
        - 'finmois' : Dernier jour du mois de vision
    """
    if not validate_vision(vision):
        raise ValueError(f"Format de vision invalide : {vision}")

    year, month = extract_year_month_int(vision)

    # Dernier jour du mois de vision
    if month == 12:
        last_day_of_month = datetime(year, month, 31)
    else:
        next_month = datetime(year, month + 1, 1) if month < 12 else datetime(year + 1, 1, 1)
        last_day_of_month = next_month - timedelta(days=1)

    # Premier jour de l'année
    first_day_of_year = datetime(year, 1, 1)

    # Premier jour du mois
    first_day_of_month = datetime(year, month, 1)

    # Dernier jour du mois précédent
    first_day_this_month = datetime(year, month, 1)
    last_day_prev_month = first_day_this_month - timedelta(days=1)

    # Formatage de toutes les dates en chaînes (YYYY-MM-DD) - Clés en minuscules uniquement
    dates = {
        'dtfin':     last_day_of_month.strftime('%Y-%m-%d'),
        'dtdeb_an':  first_day_of_year.strftime('%Y-%m-%d'),
        'dtfinmm':   last_day_of_month.strftime('%Y-%m-%d'),
        'dtfinmm1':  last_day_prev_month.strftime('%Y-%m-%d'),
        'dtdebn':    first_day_of_month.strftime('%Y-%m-%d'),
        'dtfinmn':   last_day_of_month.strftime('%Y-%m-%d'),
        'finmois':   last_day_of_month.strftime('%Y-%m-%d'),
    }

    return dates


def azure_delete_path(spark: SparkSession, path: str, logger=None) -> bool:
    """
    Supprime un chemin sur ABFS/ADLS (abfss://...) de manière récursive.
    Utilise 'urlparse' pour gérer correctement les URI Azure.

    PARAMÈTRES :
    -----------
    spark : SparkSession
        Session Spark active
    path : str
        Chemin complet (ex: abfss://container@account.../gold/file.parquet)
    logger : optionnel
        Instance de logger pour tracer l'opération

    RETOUR :
    -------
    bool
        True si suppression effectuée, False si chemin inexistant
    """
    try:
        hadoop_conf = spark._jsc.hadoopConfiguration()

        # Analyse de l'URI pour éviter les erreurs de parsing manuel
        u = urlparse(path)

        # Gestion spécifique ABFSS/ABFS
        if u.scheme in ("abfss", "abfs"):
            if not u.netloc:
                raise ValueError(f"URI ABFS invalide (netloc manquant) : {path}")

            norm_path = u.path if u.path else "/"
            full_uri = f"{u.scheme}://{u.netloc}{norm_path}"

            fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                spark._jvm.java.net.URI(full_uri),
                hadoop_conf
            )
            target = spark._jvm.org.apache.hadoop.fs.Path(full_uri)

            if not fs.exists(target):
                if logger:
                    logger.info(f"Chemin inexistant : {full_uri}")
                return False

            deleted = fs.delete(target, True)  # Récursif
            if logger:
                if deleted:
                    logger.info(f"🗑️  Supprimé : {full_uri}")
                else:
                    logger.warning(f"Suppression retournée False pour {full_uri} (peut-être utilisé ?)")
            return bool(deleted)

        # Fallback pour autres systèmes de fichiers (local, hdfs...)
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jvm.java.net.URI(path),
            hadoop_conf
        )
        target = spark._jvm.org.apache.hadoop.fs.Path(path)

        if not fs.exists(target):
            if logger:
                logger.info(f"Chemin inexistant : {path}")
            return False

        deleted = fs.delete(target, True)
        if logger:
            if deleted:
                logger.info(f"🗑️  Supprimé : {path}")
            else:
                logger.warning(f"Suppression retournée False pour {path}")
        return bool(deleted)

    except Exception as e:
        if logger:
            logger.error(f"Échec suppression pour {path} : {e}")
        raise


def write_to_layer(
    df,
    config,
    layer: str,
    filename: str,
    vision: str,
    logger=None,
    optimize: bool = False,
    zorder_cols: Optional[List[str]] = None
) -> None:
    """
    Écrivain générique pour les couches Silver/Gold du Data Lake.
    Supporte Delta, Parquet, CSV.

    FONCTIONNALITÉS :
    ----------------
    - Mode écriture dynamique (overwrite/append) depuis config.yml
    - Overwrite "safe" pour Delta (évite la duplication de partitions)
    - OPTIMIZE + ZORDER optionnels (par table)
    - Nettoyage VACUUM optionnel
    - Coalesce automatique pour les petits fichiers Gold

    PARAMÈTRES :
    -----------
    df : DataFrame
        Données à écrire
    config : ConfigLoader
        Configuration chargée
    layer : str
        'silver' ou 'gold'
    filename : str
        Nom du fichier de sortie (sans extension)
    vision : str
        Vision au format YYYYMM
    logger : optionnel
        Logger pour le suivi
    optimize : bool
        Si True, exécute OPTIMIZE sur la table Delta
    zorder_cols : List[str], optionnel
        Colonnes pour le Z-Ordering (si optimize=True)
    """

    # Résolution configuration
    output_format = config.get("output.format", "delta").lower()
    compression = config.get("output.compression", "snappy")
    mode = config.get("output.mode", "overwrite").lower()
    vacuum_hours = config.get("output.vacuum_hours", 168)
    clean = config.get("output.clean", False)  # Suppression avant écriture

    # Construction du chemin avec la nouvelle fonction
    layer_path = build_layer_path(config, layer, vision)

    # Delta = dossier, autres = fichier avec extension
    if output_format == "delta":
        output_path = f"{layer_path}/{filename}"
    else:
        output_path = f"{layer_path}/{filename}.{output_format}"

    if logger:
        logger.info(f"Écriture vers {output_path}")
        logger.info(f"Format={output_format}, Mode={mode}, Layer={layer}, Clean={clean}")


    # ================================================================
    # NETTOYAGE : Suppression préalable si demandée (Azure-safe)
    # ================================================================
    if clean:
        spark = df.sparkSession
        azure_delete_path(spark, output_path, logger)


        # ================================================================
        # OPTIMISATION COALESCE POUR COUCHE GOLD
        # ================================================================
        if layer == "gold":
            try:
                row_count = df.count()
                if row_count < 1_000_000:
                    df = df.coalesce(1)
                    if logger:
                        logger.debug(f"Coalesce à 1 partition ({row_count:,} lignes)")
                elif row_count < 10_000_000:
                    df = df.coalesce(4)
                    if logger:
                        logger.debug(f"Coalesce à 4 partitions ({row_count:,} lignes)")
            except Exception as e:
                if logger:
                    logger.warning(f"Coalesce ignoré : {e}")

    # ================================================================
    # LOGIQUE D'ÉCRITURE PAR FORMAT
    # ================================================================
    writer = df.write.mode(mode)

    # PARQUET
    if output_format == "parquet":
        writer.option("compression", compression).parquet(output_path)
        if logger:
            logger.success(f"Écriture Parquet terminée : {output_path}")

    # CSV
    elif output_format == "csv":
        writer.option("header", True).option("delimiter", ";").csv(output_path)
        if logger:
            logger.success(f"Écriture CSV terminée : {output_path}")

    # DELTA LAKE
    elif output_format == "delta":
        
        writer = writer.format("delta")
        
        # Overwrite schema seulement en mode overwrite
        if mode == "overwrite":
            writer = writer.option("overwriteSchema", "true")
            # Mode "static" pour remplacer toute la table (pas juste une partition)
            writer = writer.option("partitionOverwriteMode", "static")
        
        writer.save(output_path)

        if logger:
            logger.info("Écriture Delta terminée")

        # ================================================================
        # OPTIMISATIONS POST-ÉCRITURE (DELTA)
        # ================================================================
        
        try:
            from delta.tables import DeltaTable
        except ImportError:
            if logger:
                logger.warning("Delta Lake non disponible - optimisations ignorées")
            return
        
        # VACUUM (Nettoyage des anciennes versions)
        try:
            delta_tbl = DeltaTable.forPath(df.sparkSession, output_path)
            delta_tbl.vacuum(vacuum_hours)
            if logger:
                logger.info(f"VACUUM terminé (rétention={vacuum_hours}h)")
        except Exception as e:
            if logger:
                logger.warning(f"VACUUM ignoré : {e}")

        # OPTIMIZE + ZORDER
        if optimize:
            try:
                spark = df.sparkSession
                optimize_sql = f"OPTIMIZE delta.`{output_path}`"

                if zorder_cols:
                    invalid_cols = [c for c in zorder_cols if c not in df.columns]
                    if invalid_cols:
                        if logger:
                            logger.warning(f"Colonnes ZORDER introuvables : {invalid_cols}")
                        # Utiliser uniquement les colonnes valides
                        valid_zorder = [c for c in zorder_cols if c in df.columns]
                        if valid_zorder:
                            optimize_sql += " ZORDER BY (" + ", ".join(valid_zorder) + ")"
                    else:
                        optimize_sql += " ZORDER BY (" + ", ".join(zorder_cols) + ")"

                spark.sql(optimize_sql)

                if logger:
                    logger.success(f"OPTIMIZE terminé : {output_path}")

            except Exception as e:
                if logger:
                    logger.warning(f"OPTIMIZE échoué : {e}")

    else:
        raise ValueError(f"Format de sortie non supporté : {output_format}")

    if logger:
        logger.success(f"Données {layer.capitalize()} écrites avec succès")


def upload_log_to_datalake(
    spark,
    local_log_path: str,
    config
) -> bool:
    """
    Téléverse un fichier de log local vers le conteneur 'bronze/logs' du Data Lake.
    Supprime le fichier local après succès.

    PARAMÈTRES :
    -----------
    spark : SparkSession
        Session Spark active
    local_log_path : str
        Chemin local du fichier de log
    config : ConfigLoader
        Configuration chargée contenant environment.container et datalake.data_root

    RETOUR :
    -------
    bool
        True si succès, False sinon
    """
    import os
    
    try:
        local_path = Path(local_log_path)
        
        if not local_path.exists():
            return False
        
        # Construire le chemin avec la nouvelle structure
        container = os.getenv('DATALAKE_CONTAINER') or config.get('environment.container')
        data_root = config.get('datalake.data_root')
        datalake_log_path = f"{container}{data_root}/bronze/logs/{local_path.name}"
        
        print(f"Chemin local du fichier log : {local_path}")
        print(f"Chemin cible sur le Data Lake : {datalake_log_path}")
        
        # Lecture du fichier local
        with open(local_path, 'r', encoding="utf-8") as f:
            file_content = f.read()
        
        # Récupération du FileSystem
        hadoop_conf = spark._jsc.hadoopConfiguration()
        dest_path = spark._jvm.org.apache.hadoop.fs.Path(datalake_log_path)
        fs = dest_path.getFileSystem(hadoop_conf)
        
        # Création du flux d'écriture
        fs_output_stream = fs.create(dest_path, True)  # True = Overwrite
        output_stream_writer = spark._jvm.java.io.OutputStreamWriter(fs_output_stream, "UTF-8")
        buffered_writer = spark._jvm.java.io.BufferedWriter(output_stream_writer)
        
        # Écriture du contenu
        buffered_writer.write(file_content)
        
        buffered_writer.flush()
        buffered_writer.close()
        output_stream_writer.close()
        fs_output_stream.close()
        
        print("√ Fichier téléversé avec succès")
        
        # Suppression locale
        local_path.unlink()
        
        return True
        
    except Exception as e:
        print(f"Erreur : {e}")
        import traceback
        traceback.print_exc()
        return False