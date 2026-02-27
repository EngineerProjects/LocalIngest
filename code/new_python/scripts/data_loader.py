#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
===============================================================================
DATA LOADER — Vérificateur et chargeur Bronze
===============================================================================

Script à exécuter AVANT main.py pour garantir que tous les fichiers requis
par le pipeline sont bien présents dans la couche Bronze (ADLS).

USAGE
-----
    python scripts/data_loader.py --vision 202412
    python scripts/data_loader.py --vision 202412 --dry-run
    python scripts/data_loader.py --vision 202412 --component emissions
    python scripts/data_loader.py --vision 202412 --file segmentprdt_{vision}.csv

OPTIONS
-------
    --vision      (obligatoire) Période à traiter au format YYYYMM
    --dry-run     Vérifie sans copier (diagnostic uniquement)
    --component   Charge uniquement les fichiers d'un composant (ex: emissions, capitaux, ptf_mvt)
    --file        Charge uniquement un fichier spécifique (par pattern name)
    --config-dir  Répertoire contenant reading_config.json et data_sources.json (défaut: ./config)
    --verbose     Logs détaillés

ARCHITECTURE
------------
    data_sources.json   → définit QUOI charger et DEPUIS OÙ (source ADLS)
    reading_config.json → définit QUE doit attendre le pipeline dans Bronze

    Chemin Bronze mensuel   : {bronze_container}/construction/{YYYY}/{MM}/{fichier}
    Chemin Bronze référence : {bronze_container}/construction/ref/{fichier}

ENVIRONNEMENT
-------------
    Le script utilise DefaultAzureCredential (azure-identity).
    Sur Databricks, l'identité du cluster est utilisée automatiquement.
    Sur ADP, la Managed Identity ou le Service Principal configuré est utilisé.

    Dépendances Python :
        pip install azure-storage-file-datalake azure-identity
===============================================================================
"""

import argparse
import json
import logging
import sys
import fnmatch
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple


# ==============================================================================
# LOGGING
# ==============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("data_loader")


# ==============================================================================
# STRUCTURES DE DONNÉES
# ==============================================================================

@dataclass
class FileStatus:
    """Résultat de la vérification/chargement d'un fichier."""
    pattern:          str             # Pattern original  ex: "ipf16.csv"
    resolved_name:    str             # Nom résolu        ex: "segmentprdt_202412.csv"
    bronze_path:      str             # Chemin Bronze complet (abfss://...)
    source_path:      str             # Chemin Source complet (abfss://...)
    optional:         bool = False

    exists_in_bronze: bool            = False
    copied:           bool            = False
    skipped:          bool            = False   # dry-run ou optionnel+absent source
    error:            Optional[str]   = None


@dataclass
class LoaderReport:
    """Rapport de synthèse de l'exécution."""
    vision:          str
    dry_run:         bool
    statuses:        List[FileStatus] = field(default_factory=list)

    @property
    def already_present(self) -> List[FileStatus]:
        return [s for s in self.statuses if s.exists_in_bronze]

    @property
    def copied(self) -> List[FileStatus]:
        return [s for s in self.statuses if s.copied]

    @property
    def errors(self) -> List[FileStatus]:
        return [s for s in self.statuses if s.error]

    @property
    def skipped(self) -> List[FileStatus]:
        return [s for s in self.statuses if s.skipped and not s.error]

    @property
    def success(self) -> bool:
        """True si aucune erreur sur des fichiers non-optionnels."""
        return all(s.optional or not s.error for s in self.statuses)


# ==============================================================================
# CONFIGURATION
# ==============================================================================

class LoaderConfig:
    """
    Charge et interprète les fichiers de configuration.

    reading_config.json  → fichiers attendus dans Bronze
    data_sources.json    → sources ADLS pour chaque fichier
    """

    def __init__(self, config_dir: Path):
        self.config_dir = config_dir
        self._reading_cfg  = self._load(config_dir / "reading_config.json")
        self._sources_cfg  = self._load(config_dir / "data_sources.json")
        self._storage      = self._sources_cfg["storage"]
        self._file_sources = self._sources_cfg["files"]

    @staticmethod
    def _load(path: Path) -> Dict:
        if not path.exists():
            log.error(f"Fichier de configuration introuvable : {path}")
            sys.exit(1)
        with open(path, encoding="utf-8") as f:
            return json.load(f)

    # --- Propriétés ADLS --------------------------------------------------------

    @property
    def account_name(self) -> str:
        return self._storage["account_name"]

    @property
    def bronze_container(self) -> str:
        return self._storage["bronze_container"]

    @property
    def source_container(self) -> str:
        return self._storage["source_container"]

    @property
    def monthly_path_template(self) -> str:
        return self._storage["monthly_path"]   # ex: "construction/{year}/{month}"

    @property
    def reference_path_template(self) -> str:
        return self._storage["reference_path"] # ex: "construction/ref"

    # --- Résolution des chemins -------------------------------------------------

    def bronze_url(self, container: str) -> str:
        return f"abfss://{container}@{self.account_name}"

    def resolve_bronze_path(self, pattern: str, vision: str) -> str:
        """Construit le chemin Bronze complet pour un fichier."""
        resolved_name = pattern.replace("{vision}", vision)
        location_type = self._get_location_type(pattern)

        year, month = vision[:4], vision[4:]

        if location_type == "monthly":
            folder = self.monthly_path_template.format(year=year, month=month)
        else:
            folder = self.reference_path_template

        base_url = self.bronze_url(self.bronze_container)
        return f"{base_url}/{folder}/{resolved_name}"

    def resolve_source_path(self, pattern: str, vision: str) -> Optional[str]:
        """Construit le chemin Source complet pour un fichier."""
        source_entry = self._find_source_entry(pattern)
        if source_entry is None:
            return None

        src_path_tpl = source_entry["source_path"]
        vision_in_source = source_entry.get("vision_in_source", False)

        resolved_src = src_path_tpl.replace("{vision}", vision) if vision_in_source else src_path_tpl
        base_url = self.bronze_url(self.source_container)
        return f"{base_url}/{resolved_src}"

    def _get_location_type(self, pattern: str) -> str:
        """Déduit location_type depuis data_sources.json ou reading_config.json."""
        src = self._find_source_entry(pattern)
        if src:
            return src.get("location_type", "reference")
        # Fallback : parcourir reading_config
        for group in self._reading_cfg["file_groups"].values():
            if not isinstance(group, dict):
                continue
            if pattern in group.get("file_patterns", []):
                return group.get("location_type", "reference")
        return "reference"

    def _find_source_entry(self, pattern: str) -> Optional[Dict]:
        """Trouve l'entrée dans data_sources.json pour un pattern donné."""
        # Correspondance exacte
        if pattern in self._file_sources:
            entry = self._file_sources[pattern]
            return entry if isinstance(entry, dict) else None
        # Correspondance glob (ex: "ptgst_*.csv")
        for key, entry in self._file_sources.items():
            if not isinstance(entry, dict):
                continue
            if fnmatch.fnmatch(pattern, key) or fnmatch.fnmatch(key, pattern):
                return entry
        return None

    def is_optional(self, pattern: str) -> bool:
        src = self._find_source_entry(pattern)
        if src:
            return src.get("optional", False)
        return False

    def get_source_format(self, pattern: str) -> str:
        """
        Détermine le format du fichier source : 'csv' ou 'parquet'.

        Règle simple :
        - Si source_path se termine par '.csv'  → format CSV  (fichier unique)
        - Sinon                                 → format Parquet (dossier de partitions)

        Un champ explicite ``source_format`` dans data_sources.json peut
        toujours forcer le comportement si nécessaire.
        """
        src = self._find_source_entry(pattern)
        if src:
            # Override explicite en priorité
            if "source_format" in src:
                return src["source_format"].lower()
            # Détection basée sur l'extension du chemin source
            src_path = src.get("source_path", "")
            if src_path.lower().endswith(".csv"):
                return "csv"
        # Par défaut : parquet (dossier distribué)
        return "parquet"


    # --- Énumération des fichiers requis ----------------------------------------

    def get_all_patterns(self, component: Optional[str] = None) -> List[str]:
        """
        Retourne la liste des file_patterns à vérifier.

        Si component est spécifié, filtre sur les file_groups du composant.
        """
        file_groups = self._reading_cfg.get("file_groups", {})
        components  = self._reading_cfg.get("components", {})

        # Filtrer par composant si demandé
        if component:
            comp_cfg = components.get(component)
            if not comp_cfg:
                log.warning(f"Composant '{component}' non trouvé dans reading_config.json. "
                            f"Composants disponibles : {list(components.keys())}")
                return []
            required_groups = comp_cfg.get("required_file_groups", [])
        else:
            required_groups = None  # Tous les groupes

        patterns = []
        for group_name, group in file_groups.items():
            if not isinstance(group, dict):
                continue
            if required_groups is not None and group_name not in required_groups:
                continue
            for pat in group.get("file_patterns", []):
                patterns.append(pat)

        return patterns


# ==============================================================================
# CLIENT ADLS
# ==============================================================================

class ADLSClient:
    """
    Abstraction légère au-dessus du SDK Azure Data Lake Storage Gen2.

    Utilise DefaultAzureCredential — aucune configuration manuelle requise
    sur un environnement Azure configuré (Managed Identity, SP, etc.).
    """

    def __init__(self, account_name: str):
        try:
            from azure.identity import DefaultAzureCredential
            from azure.storage.filedatalake import DataLakeServiceClient
        except ImportError:
            log.error(
                "Dépendances manquantes. Installer avec : "
                "pip install azure-storage-file-datalake azure-identity"
            )
            sys.exit(1)

        credential = DefaultAzureCredential()
        account_url = f"https://{account_name}"
        self._svc = DataLakeServiceClient(account_url=account_url, credential=credential)
        log.debug(f"Connecté à ADLS : {account_url}")

    def exists(self, container: str, path: str) -> bool:
        """Vérifie si un fichier CSV unique existe dans ADLS."""
        try:
            fs = self._svc.get_file_system_client(container)
            file_client = fs.get_file_client(path)
            file_client.get_file_properties()
            return True
        except Exception:
            return False

    def exists_directory(self, container: str, path: str) -> bool:
        """
        Vérifie si un dossier parquet existe et contient au moins un fichier.

        Un dossier parquet ADLS est considéré présent si :
        - Le chemin existe en tant que répertoire
        - Il contient au moins un fichier (partition .parquet ou _SUCCESS)
        """
        try:
            fs = self._svc.get_file_system_client(container)
            # get_paths retourne les entrées du dossier
            paths = list(fs.get_paths(path=path, max_results=1))
            return len(paths) > 0
        except Exception:
            return False

    def copy(self, src_container: str, src_path: str,
             dst_container: str, dst_path: str) -> None:
        """
        Copie un fichier CSV unique d'un conteneur vers un autre.

        Stratégie : download en mémoire + upload.
        Pour les très gros fichiers (>500 Mo), envisager AzCopy ou serverside copy.
        """
        # Lecture depuis la source
        src_fs   = self._svc.get_file_system_client(src_container)
        src_file = src_fs.get_file_client(src_path)
        stream   = src_file.download_file()
        data     = stream.readall()

        # Création des dossiers parents dans Bronze si besoin
        dst_fs = self._svc.get_file_system_client(dst_container)
        parent = str(Path(dst_path).parent)
        try:
            dst_fs.create_directory(parent)
        except Exception:
            pass  # Le dossier existe déjà — OK

        # Écriture dans Bronze
        dst_file = dst_fs.get_file_client(dst_path)
        dst_file.upload_data(data, overwrite=True)

    def copy_directory(self, src_container: str, src_dir: str,
                       dst_container: str, dst_dir: str) -> int:
        """
        Copie récursive d'un dossier parquet (toutes les partitions).

        Liste tous les fichiers du dossier source (récursivement) et les
        copie un à un dans le dossier cible Bronze, en préservant la
        structure relative des sous-dossiers.

        Retourne le nombre de fichiers copiés.
        """
        src_fs = self._svc.get_file_system_client(src_container)
        dst_fs = self._svc.get_file_system_client(dst_container)

        # Lister tous les fichiers du dossier source (récursif)
        all_paths = list(src_fs.get_paths(path=src_dir, recursive=True))
        files     = [p for p in all_paths if not p.is_directory]

        if not files:
            raise FileNotFoundError(f"Dossier parquet vide ou introuvable : {src_dir}")

        count = 0
        # Normaliser le préfixe source pour calculer les chemins relatifs
        src_prefix = src_dir.rstrip("/")

        for file_path_obj in files:
            full_src_path = str(file_path_obj.name).lstrip("/")
            # Chemin relatif depuis la racine du dossier source
            rel_path = full_src_path[len(src_prefix):].lstrip("/")
            full_dst_path = f"{dst_dir.rstrip('/')}/{rel_path}"

            # Télécharger et renécrire
            src_file = src_fs.get_file_client(full_src_path)
            stream   = src_file.download_file()
            data     = stream.readall()

            # Créer le sous-dossier destination si besoin
            parent = str(Path(full_dst_path).parent)
            try:
                dst_fs.create_directory(parent)
            except Exception:
                pass

            dst_file = dst_fs.get_file_client(full_dst_path)
            dst_file.upload_data(data, overwrite=True)
            count += 1

        return count

    @staticmethod
    def _parse_abfss(url: str) -> Tuple[str, str]:
        """
        Décompose une URL abfss:// en (container, path_sans_base).
        ex: abfss://bronze@storage.dfs.core.windows.net/construction/ref/foo.csv
            → ("bronze", "construction/ref/foo.csv")
        """
        # Enlever le préfixe abfss://
        without_scheme = url.replace("abfss://", "")
        # Format: container@account/path
        container, rest = without_scheme.split("@", 1)
        _, path = rest.split("/", 1)
        return container, path


# ==============================================================================
# MOTEUR PRINCIPAL
# ==============================================================================

class DataLoader:
    """Orchestre la vérification et le chargement des fichiers Bronze."""

    def __init__(self, config: LoaderConfig, adls: ADLSClient, dry_run: bool = False):
        self.config  = config
        self.adls    = adls
        self.dry_run = dry_run

    def run(
        self,
        vision:    str,
        patterns:  List[str],
        verbose:   bool = False
    ) -> LoaderReport:
        """
        Lance la vérification et le chargement pour tous les patterns.

        Paramètres
        ----------
        vision   : Période YYYYMM
        patterns : Liste des file_patterns à traiter
        verbose  : Logs détaillés par fichier

        Retour
        ------
        LoaderReport avec le statut de chaque fichier
        """
        report = LoaderReport(vision=vision, dry_run=self.dry_run)

        total = len(patterns)
        log.info(f"{'[DRY-RUN] ' if self.dry_run else ''}Vérification de {total} fichier(s) pour vision={vision}")
        log.info("─" * 70)

        for i, pattern in enumerate(patterns, 1):
            status = self._process_one(pattern, vision, verbose, pos=i, total=total)
            report.statuses.append(status)

        return report

    def _process_one(
        self, pattern: str, vision: str,
        verbose: bool, pos: int, total: int
    ) -> FileStatus:
        """Traite un fichier/dossier : vérifie l'existence, copie si nécessaire."""
        bronze_url    = self.config.resolve_bronze_path(pattern, vision)
        source_url    = self.config.resolve_source_path(pattern, vision)
        optional      = self.config.is_optional(pattern)
        source_format = self.config.get_source_format(pattern)
        resolved      = pattern.replace("{vision}", vision)

        bk_container, bk_path = ADLSClient._parse_abfss(bronze_url)
        prefix = f"[{pos:02d}/{total:02d}]"
        fmt_tag = "[parquet]" if source_format == "parquet" else "[csv]    "

        status = FileStatus(
            pattern=pattern,
            resolved_name=resolved,
            bronze_path=bronze_url,
            source_path=source_url or "N/A",
            optional=optional,
        )

        # --- 1. Vérifier existence dans Bronze ----------------------------------
        if source_format == "parquet":
            status.exists_in_bronze = self.adls.exists_directory(bk_container, bk_path)
        else:
            status.exists_in_bronze = self.adls.exists(bk_container, bk_path)

        if status.exists_in_bronze:
            tag = "✅ PRÉSENT "
            msg = f"{prefix} {tag} {fmt_tag} {resolved}"
            log.info(msg) if verbose else log.debug(msg)
            return status

        # --- 2. Fichier absent — décider quoi faire -----------------------------
        tag_absent = "⭕ OPTIONNEL" if optional else "❌ MANQUANT "
        log.info(f"{prefix} {tag_absent} {fmt_tag} {resolved}  →  Bronze: {bronze_url}")

        if source_url is None:
            msg = f"Aucune source définie dans data_sources.json pour '{pattern}'"
            if optional:
                log.warning(f"         {msg} — ignoré (optionnel)")
                status.skipped = True
            else:
                log.error(f"         {msg}")
                status.error = msg
            return status

        # --- 3. Copier depuis la source -----------------------------------------
        src_container, src_path = ADLSClient._parse_abfss(source_url)
        log.info(f"         Source  : {source_url}")
        log.info(f"         Bronze  : {bronze_url}")

        if self.dry_run:
            log.info(f"         [DRY-RUN] Copie ignorée")
            status.skipped = True
            return status

        try:
            if source_format == "parquet":
                # Copie récursive du dossier de partitions
                n = self.adls.copy_directory(src_container, src_path, bk_container, bk_path)
                log.info(f"         📥 Dossier parquet copié ({n} fichier(s))")
            else:
                # Copie d'un fichier CSV unique
                self.adls.copy(src_container, src_path, bk_container, bk_path)
                log.info(f"         📥 Fichier CSV copié")
            status.copied = True
        except Exception as exc:
            msg = f"Échec de la copie de '{resolved}' ({source_format}) : {exc}"
            if optional:
                log.warning(f"         {msg} — ignoré (optionnel)")
                status.skipped = True
            else:
                log.error(f"         {msg}")
                status.error = msg

        return status


# ==============================================================================
# RAPPORT
# ==============================================================================

def print_report(report: LoaderReport) -> None:
    """Affiche le rapport de synthèse."""
    log.info("")
    log.info("=" * 70)
    log.info(f"  RAPPORT DATA LOADER — vision={report.vision}"
             + (" [DRY-RUN]" if report.dry_run else ""))
    log.info("=" * 70)
    log.info(f"  ✅  Déjà présents dans Bronze  : {len(report.already_present):3d}")
    log.info(f"  📥  Copiés depuis la source    : {len(report.copied):3d}")
    log.info(f"  ⏭   Ignorés (optionnel/dry-run): {len(report.skipped):3d}")
    log.info(f"  ❌  Erreurs                    : {len(report.errors):3d}")
    log.info("─" * 70)

    if report.errors:
        log.error("  FICHIERS EN ERREUR (non-optionnels) :")
        for s in report.errors:
            log.error(f"    • {s.resolved_name}")
            log.error(f"      Bronze  : {s.bronze_path}")
            log.error(f"      Source  : {s.source_path}")
            log.error(f"      Erreur  : {s.error}")
        log.info("─" * 70)

    if report.success:
        log.info("  ✅  Bronze prêt — le pipeline peut démarrer.")
    else:
        log.error("  ❌  Des fichiers obligatoires sont manquants.")
        log.error("      Corriger les erreurs avant de lancer main.py.")

    log.info("=" * 70)


# ==============================================================================
# POINT D'ENTRÉE
# ==============================================================================

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Vérifie et charge les fichiers Bronze depuis ADLS avant l'exécution du pipeline."
    )
    parser.add_argument(
        "--vision", required=True,
        help="Période à traiter au format YYYYMM (ex: 202412)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Vérifie uniquement, sans copier aucun fichier"
    )
    parser.add_argument(
        "--component", default=None,
        help="Limite la vérification à un composant (ex: emissions, capitaux, ptf_mvt)"
    )
    parser.add_argument(
        "--file", default=None,
        help="Vérifie/charge un seul fichier par son pattern (ex: segmentprdt_{vision}.csv)"
    )
    parser.add_argument(
        "--config-dir", default="config",
        help="Répertoire des fichiers de configuration (défaut: ./config)"
    )
    parser.add_argument(
        "--verbose", action="store_true",
        help="Afficher les détails pour les fichiers déjà présents"
    )
    return parser.parse_args()


def validate_vision(vision: str) -> None:
    """Vérifie que la vision est au format YYYYMM."""
    if len(vision) != 6 or not vision.isdigit():
        log.error(f"Format de vision invalide : '{vision}'. Attendu : YYYYMM (ex: 202412)")
        sys.exit(1)
    year, month = int(vision[:4]), int(vision[4:])
    if not (2000 <= year <= 2100) or not (1 <= month <= 12):
        log.error(f"Vision hors plage valide : année={year}, mois={month}")
        sys.exit(1)


def main() -> None:
    args = parse_args()

    # Validation de la vision
    validate_vision(args.vision)

    if args.verbose:
        log.setLevel(logging.DEBUG)

    log.info(f"Data Loader — vision={args.vision}"
             + (" [DRY-RUN]" if args.dry_run else ""))

    # Chargement de la configuration
    config_dir = Path(args.config_dir)
    config = LoaderConfig(config_dir)

    # Détermination des fichiers à traiter
    if args.file:
        # Un seul fichier demandé
        patterns = [args.file]
        log.info(f"Mode fichier unique : {args.file}")
    else:
        patterns = config.get_all_patterns(component=args.component)
        if args.component:
            log.info(f"Composant filtré : {args.component} ({len(patterns)} fichier(s))")
        else:
            log.info(f"Tous les composants : {len(patterns)} fichier(s) à vérifier")

    if not patterns:
        log.warning("Aucun fichier à traiter. Vérifier la configuration.")
        sys.exit(0)

    # Connexion ADLS
    adls = ADLSClient(config.account_name)

    # Exécution
    loader = DataLoader(config=config, adls=adls, dry_run=args.dry_run)
    report = loader.run(vision=args.vision, patterns=patterns, verbose=args.verbose)

    # Rapport
    print_report(report)

    # Code de sortie
    sys.exit(0 if report.success else 1)


if __name__ == "__main__":
    main()
