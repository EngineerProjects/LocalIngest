# Pipeline de Données Construction

Pipeline de données PySpark pour les données d'assurance Construction (PTF_MVT, Capitaux, Émissions).

---

## Vue d'Ensemble

Transforme les données CSV brutes via une architecture médaillon Bronze→Silver→Gold :

| Couche     | Contenu                                   |
| ---------- | ----------------------------------------- |
| **Bronze** | Fichiers CSV bruts (mensuels + référence) |
| **Silver** | Parquet nettoyé avec schémas              |
| **Gold**   | Analytique prêt pour le métier            |

---

## Démarrage Rapide

```bash
# Exécuter tous les composants activés
python main.py --vision 202509

# Exécuter un composant spécifique
python main.py --vision 202509 --component ptf_mvt
python main.py --vision 202509 --component capitaux
python main.py --vision 202509 --component emissions
```

---

## Jeux de Données en Sortie

| #   | Pipeline  | Jeu de Données                    | Couche |
| --- | --------- | --------------------------------- | ------ |
| 1   | PTF_MVT   | `mvt_ptf_{vision}`                | Gold   |
| 2   | PTF_MVT   | `ird_risk_q45_{vision}`           | Gold   |
| 3   | PTF_MVT   | `ird_risk_q46_{vision}`           | Gold   |
| 4   | Capitaux  | `az_azec_capitaux_{vision}`       | Gold   |
| 5   | Emissions | `primes_emises_{vision}_pol_garp` | Gold   |
| 6   | Emissions | `primes_emises_{vision}_pol`      | Gold   |

---

## Structure du Projet

```
new_python/
├── config/                     # Fichiers de configuration
│   ├── config.yml              # Chemins, Spark, logging
│   ├── reading_config.json     # Modèles de fichiers et schémas
│   ├── schemas.py              # Définitions de schémas PySpark
│   ├── constants.py            # Constantes métier
│   └── transformations/        # JSONs de logique métier
│
├── src/                        # Pipeline principal
│   ├── reader.py               # Lecteurs Bronze/Silver
│   ├── ptf_mvt_run.py          # Orchestrateur PTF_MVT
│   ├── capitaux_run.py         # Orchestrateur Capitaux
│   ├── emissions_run.py        # Orchestrateur Émissions
│   └── processors/             # Processeurs ETL
│
├── utils/                      # Utilitaires
│   ├── loaders/                # Chargeurs de config
│   ├── logger.py               # Logging structuré
│   ├── helpers.py              # Aides dates/chemins
│   └── transformations/        # Fonctions de transformation
│
├── notebooks/                  # Notebooks de test
│   └── ptf_mvt/                # Tests des composants
│
├── docs/                       # Documentation
│   ├── workflows/              # Flux de travail du pipeline
│   ├── configs/                # Documentation de configuration
│   └── ...                     # Guides métier
│
└── main.py                     # Point d'entrée
```

---

## Documentation

| Document                                                      | Description                          |
| ------------------------------------------------------------- | ------------------------------------ |
| [Flux PTF_MVT](docs/workflows/PTF_MVT_Workflow.md)            | Mouvements de portefeuille           |
| [Flux Capitaux](docs/workflows/Capitaux_Workflow.md)          | Extraction de capitaux               |
| [Flux Émissions](docs/workflows/Emissions_Workflow.md)        | Émissions de primes                  |
| [Guide de Configuration](docs/configs/Configuration_Guide.md) | Référence de configuration           |
| [Catalogue de Données](docs/configs/Data_Catalog.md)          | Tables d'entrée/sortie               |
| [Calculs Assurance](docs/calculs_assurance_construction.md)   | Formules techniques & logique métier |

---

## Configuration

| Fichier                  | Usage                        |
| ------------------------ | ---------------------------- |
| `config.yml`             | Chemins, Spark, logging      |
| `reading_config.json`    | Modèles de fichiers, schémas |
| `schemas.py`             | Définitions de types PySpark |
| `constants.py`           | Constantes métier            |
| `*_transformations.json` | Règles métier                |

---

## Prérequis

- Python 3.8+
- PySpark 3.x
- Accès Azure Data Lake (ABFSS)