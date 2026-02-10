"""
Utilitaires de Journalisation (Logging) pour le Pipeline de Construction.

Fournit un système de journalisation simple et propre, avec affichage
simultané dans la console et dans un fichier, incluant des marqueurs de section.
"""

import logging
import sys
from pathlib import Path
from datetime import datetime
from typing import Optional


class PipelineLogger:
    """
    Logger pour les opérations du pipeline avec un formatage amélioré.
    Facilite le suivi de l'exécution et le débogage.
    """

    def __init__(
        self,
        name: str,
        log_file: Optional[str] = None,
        level: str = "INFO"
    ):
        """
        Initialise le logger du pipeline.

        PARAMÈTRES :
        -----------
        name : str
            Nom du logger (généralement le nom du module)
        log_file : str, optionnel
            Chemin vers le fichier de log
        level : str
            Niveau de journalisation (DEBUG, INFO, WARNING, ERROR) - Par défaut: INFO
        """
        self.logger = logging.getLogger(name)
        self.logger.setLevel(getattr(logging, level.upper()))

        # Éviter la duplication des handlers si le logger existe déjà
        if self.logger.handlers:
            return

        # Création du formatteur
        # Format: Date Heure - Nom - Niveau - Message
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        # Handler pour la console (Sortie standard)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

        # Handler pour le fichier (si un chemin est fourni)
        if log_file:
            log_path = Path(log_file)
            # Création automatique du dossier parent s'il n'existe pas
            log_path.parent.mkdir(parents=True, exist_ok=True)

            file_handler = logging.FileHandler(log_file, mode='w', encoding='utf-8')
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)

    def debug(self, message: str) -> None:
        """Enregistre un message de débogage (DEBUG)."""
        self.logger.debug(message)

    def info(self, message: str) -> None:
        """Enregistre un message d'information (INFO)."""
        self.logger.info(message)

    def warning(self, message: str) -> None:
        """Enregistre un avertissement (WARNING)."""
        self.logger.warning(message)

    def error(self, message: str) -> None:
        """Enregistre une erreur (ERROR)."""
        self.logger.error(message)

    def critical(self, message: str) -> None:
        """Enregistre une erreur critique (CRITICAL)."""
        self.logger.critical(message)

    def section(self, title: str) -> None:
        """
        Affiche un séparateur de section pour améliorer la lisibilité des logs.

        PARAMÈTRES :
        -----------
        title : str
            Titre de la section
        """
        separator = "=" * 80
        self.logger.info(separator)
        self.logger.info(f"  {title}")
        self.logger.info(separator)

    def step(self, step_number: float, description: str) -> None:
        """
        Enregistre une étape numérotée dans le pipeline.

        PARAMÈTRES :
        -----------
        step_number : float
            Numéro de l'étape (peut être un décimal, ex: 1.1)
        description : str
            Description de l'action en cours
        """
        self.logger.info(f"ÉTAPE {step_number}: {description}")

    def success(self, message: str) -> None:
        """Enregistre un message de succès avec un préfixe visuel."""
        self.logger.info(f"✓ SUCCÈS : {message}")

    def failure(self, message: str) -> None:
        """Enregistre un message d'échec avec un préfixe visuel."""
        self.logger.error(f"✗ ÉCHEC : {message}")

    def timer_start(self, operation: str) -> datetime:
        """
        Démarre le chronométrage d'une opération.

        PARAMÈTRES :
        -----------
        operation : str
            Nom de l'opération à chronométrer

        RETOUR :
        -------
        datetime
            Horodatage de début
        """
        start_time = datetime.now()
        self.logger.info(f"Début : {operation}")
        return start_time

    def timer_end(self, operation: str, start_time: datetime) -> None:
        """
        Arrête le chronométrage et enregistre la durée écoulée.

        PARAMÈTRES :
        -----------
        operation : str
            Nom de l'opération terminée
        start_time : datetime
            Horodatage de début (retourné par timer_start)
        """
        duration = (datetime.now() - start_time).total_seconds()
        self.logger.info(f"Terminé : {operation} (Durée : {duration:.2f}s)")


def get_logger(
    name: str,
    log_file: Optional[str] = None,
    level: str = "INFO"
) -> PipelineLogger:
    """
    Fonction utilitaire pour obtenir une instance de logger configurée.

    PARAMÈTRES :
    -----------
    name : str
        Nom du logger
    log_file : str, optionnel
        Chemin vers le fichier de log
    level : str
        Niveau de logging

    RETOUR :
    -------
    PipelineLogger
        Instance du logger prête à l'emploi

    EXEMPLE :
    --------
    >>> logger = get_logger(__name__, 'logs/pipeline.log')
    >>> logger.info('Démarrage du pipeline')
    >>> logger.success('Données chargées')
    """
    return PipelineLogger(name, log_file, level)
