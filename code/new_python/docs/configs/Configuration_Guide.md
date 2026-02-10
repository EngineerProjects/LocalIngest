# Guide de Configuration

## Vue d'Ensemble

Le pipeline utilise une architecture **pilotée par la configuration**, toute la logique métier étant externalisée du code.

---

## Fichiers de Configuration

| Fichier                         | Emplacement               | Objectif                               |
| ------------------------------- | ------------------------- | -------------------------------------- |
| **config.yml**                  | `config/`                 | Chemins, paramètres Spark, logging     |
| **reading_config.json**         | `config/`                 | Modèles de fichiers et options lecture |
| **schemas.py**                  | `config/`                 | Définitions schémas PySpark            |
| **constants.py**                | `config/`                 | Constantes métier et listes exclusion  |
| **az_transformations.json**     | `config/transformations/` | Logique canal AZ                       |
| **azec_transformations.json**   | `config/transformations/` | Logique canal AZEC                     |
| **business_rules.json**         | `config/transformations/` | Règles métier partagées                |
| **emissions_config.json**       | `config/transformations/` | Filtres et mappages Émissions          |
| **consolidation_mappings.json** | `config/transformations/` | Harmonisation schéma AZ/AZEC           |

---

## 1. Paramètres Globaux (config.yml)

### Sections Clés

| Section              | Objectif                                      |
| -------------------- | --------------------------------------------- |
| `datalake.base_path` | Chemin racine pour couches bronze/silver/gold |
| `output.format`      | Format de sortie (delta, parquet)             |
| `components`         | Activer/désactiver composants pipeline        |
| `spark.config`       | Configuration session Spark                   |
| `logging`            | Niveau de log et emplacement fichier          |
| `vision.validation`  | Validation plage d'années                     |

### Configuration Sortie

| Clé                  | Défaut      | Description                                |
| -------------------- | ----------- | ------------------------------------------ |
| `output.format`      | `delta`     | Format sortie (delta pour garanties ACID)  |
| `output.mode`        | `overwrite` | Mode écriture (overwrite, append)          |
| `output.compression` | `snappy`    | Codec compression                          |
| `output.clean`       | `true`      | Nettoyer données existantes avant écriture |

---

## 2. Lecture de Fichiers (reading_config.json)

### Structure Groupe de Fichiers

| Champ                   | Description                       |
| ----------------------- | --------------------------------- |
| `description`           | Description lisible               |
| `file_patterns`         | Patterns Glob pour correspondance |
| `schema`                | Référence vers schéma schemas.py  |
| `read_options.format`   | csv, parquet                      |
| `read_options.sep`      | Séparateur colonnes               |
| `read_options.header`   | A une ligne d'entête              |
| `read_options.encoding` | ISO-8859-15, UTF-8                |
| `location_type`         | monthly (mensuel) ou reference    |

### Types d'Emplacement

| Type          | Modèle Chemin         |
| ------------- | --------------------- |
| **monthly**   | `bronze/{YYYY}/{MM}/` |
| **reference** | `bronze/ref/`         |

### Groupes de Fichiers Actuels (45 total)

- Fichiers IMS : `ipf`, `ipfm99_az`, `ipfspe_*`
- OneBI : `rf_fr1_prm_dtl_midcorp_m`
- AZEC : `polic_cu_azec`, `capitxcu_azec`, `incendcu_azec`, etc.
- Risque IRD : `ird_risk_q45`, `ird_risk_q46`, `ird_risk_qan`
- Référence : `segmentprdt`, `ptgst`, `cproduit`, `prdcap`, etc.
- Nouvelles tables : `ref_mig_azec_vs_ims`, `indices`, `mapping_isic_*`, `ref_isic`

---

## 3. Schémas (schemas.py)

Schémas PySpark explicites pour la sécurité de type.

### Registre de Schémas

Mappe les noms de groupes de fichiers vers les StructType :
- `ipf` → IPF_AZ_SCHEMA
- `capitxcu_azec` → CAPITXCU_SCHEMA
- etc.

### Avantages

- ✅ Validation et sécurité de type
- ✅ Meilleure performance (pas d'inférence de schéma)
- ✅ Dictionnaire de données auto-documenté

---

## 4. Constantes (constants.py)

### Constantes Métier

| Constante | Valeurs                            |
| --------- | ---------------------------------- |
| DIRCOM    | AZ="AZ", AZEC="AZEC" (chaine)      |
| CDPOLE    | Agent="1", Courtage="3" (chaine)   |
| CMARCH    | Construction="6"                   |
| CSEG      | Segment="2" (segment Construction) |

### Listes d'Exclusion

| Liste               | Compte | Utilisé Dans   |
| ------------------- | ------ | -------------- |
| Exclusions NOINT    | 20     | Filtres AZ     |
| Intermédiaires AZEC | 2      | Filtres AZEC   |
| Produits Exclus     | 1      | Tous pipelines |

---

## 5. JSONs de Transformation

### az_transformations.json

| Section              | Objectif                                   |
| -------------------- | ------------------------------------------ |
| `column_selection`   | Passthrough, renommage, colonnes calculées |
| `business_filters`   | Filtres marché, segment, statut            |
| `capital_extraction` | Extraction capitaux par mots-clés          |
| `movements`          | Mappages colonnes date                     |

### business_rules.json

| Section              | Objectif                 |
| -------------------- | ------------------------ |
| `coassurance_config` | Détermination type COASS |
| `az_transform_steps` | Calculs règles métier    |
| `gestsit_rules`      | Mises à jour statut      |

### azec_transformations.json

| Section              | Objectif                          |
| -------------------- | --------------------------------- |
| `column_selection`   | Configuration colonnes AZEC       |
| `business_filters`   | Filtres spécifiques AZEC          |
| `migration_handling` | Logique vision > 202009           |
| `movement_products`  | Liste 47 produits pour mouvements |

### emissions_config.json

| Section                   | Objectif                |
| ------------------------- | ----------------------- |
| `excluded_intermediaries` | 15 codes intermédiaires |
| `excluded_guarantees`     | 4 codes garanties       |
| `channel_mapping`         | cd_niv_2_stc → CDPOLE   |

---

## Dépannage

| Problème                     | Solution                                      |
| ---------------------------- | --------------------------------------------- |
| Échec validation schéma      | Vérifier correspondance exacte noms colonnes  |
| Filtre non appliqué          | Vérifier existence colonne avant étape filtre |
| Fichier non trouvé           | Vérifier correspondance pattern glob          |
| Transformation retourne null | Ajouter valeurs par défaut                    |

---

**Dernière Mise à Jour** : 06/02/2026
**Version** : 1.1
