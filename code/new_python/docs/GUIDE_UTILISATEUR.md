# 📘 Guide du Développeur & Tutoriel de Configuration

Ce guide est conçu pour vous apprendre à **comprendre, configurer et modifier** le pipeline de données Construction sans avoir nécessairement besoin de modifier le code Python.

## 🌟 1. Vue d'Ensemble du Pipeline

Le pipeline suit une architecture standard "Médaillon" (Bronze → Silver → Gold) adaptée aux besoins de l'assurance construction.

### Architecture Simplifiée

```mermaid
graph LR
    Input[Fichiers CSV Bruts] --> Bronze
    Bronze --> Reader[Lecteur Intelligent]
    Reader --> Silver[Données Nettoyées]
    Silver --> Transformations[Calculs Métier]
    Transformations --> Gold[Données Finales]
    Config[Configuration JSON/YAML] -.-> Reader
    Config -.-> Transformations
```

1.  **Bronze (Entrée)** : Les fichiers bruts déposés par les systèmes amont (IMS, OneBI).
2.  **Lecteur (Reader)** : Un composant intelligent qui lit, nettoie et standardise les données en se basant sur la configuration.
3.  **Silver (Intermédiaire)** : Données propres, typées (dates, nombres) et filtrées.
4.  **Gold (Sortie)** : Données enrichies avec les règles métier (Capital, Mouvements, etc.), prêtes pour l'analyse.

---

## 🛠️ 2. Guide des Transformations Rapides (JSON)

La plupart des règles métier sont définies dans des fichiers JSON situés dans `config/transformations/`. Vous pouvez modifier ces règles **sans toucher au code Python**.

### Exemple : Modifier un Filtre Métier

Supposons que vous vouliez changer les codes nature acceptés pour le portefeuille AZ.

1.  Ouvrez `config/transformations/az_transformations.json`.
2.  Cherchez la section `business_filters`.

```json
"business_filters": [
    {
        "filter_name": "nature_police",
        "column": "cdnatp",
        "operator": "in",
        "values": ["R", "O", "T", "C"]  <-- Modifiez cette liste
    }
]
```

3.  Ajoutez ou retirez un code (ex: ajoutez `"D"`).
4.  Sauvegardez. Le pipeline prendra en compte ce changement à la prochaine exécution.

### Exemple : Changer un Mot-clé de Recherche de Capital

Si un nouveau libellé apparaît pour le calcul du SMP :

1.  Ouvrez `config/transformations/az_transformations.json`.
2.  Cherchez `capital_extraction`.

```json
"smp_global": {
    "keywords": [
        "SMP GLOBAL",
        "SMP RETENU",
        "SINISTRE MAXIMUM POSSIBLE",
        "NOUVEAU LIBELLÉ ICI"  <-- Ajoutez votre mot-clé
    ],
    "priority": 1
}
```

---

## 📥 3. Gestion des Entrées (`reading_config.json`)

Ce fichier est le **cerveau** du lecteur de données. Il dit au pipeline *quoi* lire et *comment* le lire.

### Structure d'un Groupe de Fichiers

Chaque type de fichier est défini par un bloc dans `file_groups` :

```json
"ipf": {
    "description": "Fichiers portefeuille",
    "file_patterns": ["ipf16.csv", "ipf36.csv"],
    "schema": "ipf",
    "read_options": {
        "sep": "|",
        "encoding": "LATIN9"
    },
    "dynamic_columns": [...]
}
```

### Comment Ajouter ou Retirer des Colonnes ?

Le pipeline utilise des définitions de colonnes centralisées dans `config/column_definitions.py`.

1.  **Pour retirer une colonne** :
    *   Allez dans `config/column_definitions.py`.
    *   Trouvez la définition du schéma (ex: `IPF_SCHEMA = "..."`).
    *   Supprimez la colonne de la chaîne de caractères (format DDL SQL).
    *   Le lecteur ignorera désormais cette colonne lors de la lecture.

2.  **Pour ajouter une colonne existante dans le CSV** :
    *   Allez dans `config/column_definitions.py`.
    *   Ajoutez le nom et le type dans la chaîne DDL : `nom_colonne TYPE,` (ex: `nouvelle_col STRING,`).
    *   **Attention** : Le nom doit correspondre EXACTEMENT (insensible à la casse) au nom dans le fichier CSV.

**Exemple concret** :

Fichier : `config/column_definitions.py`
```python
IPF_SCHEMA = """
    cdpole STRING,
    nopol STRING,
    dtcrepol DATE,
    nouvelle_colonne STRING  -- Ajoutée ici
"""
```

Le pipeline lira maintenant `nouvelle_colonne` depuis les fichiers IPF.
    *   Trouvez la définition du schéma (ex: `IPF_SCHEMA = "..."`).
    *   Supprimez la colonne de la chaîne de caractères (format DDL SQL).
    *   Le lecteur ignorera désormais cette colonne lors de la lecture.

2.  **Pour ajouter une colonne existante dans le CSV** :
    *   Allez dans `config/column_definitions.py`.
    *   Ajoutez le nom et le type dans la chaîne DDL : `nom_colonne TYPE,` (ex: `nouvelle_col STRING,`).
    *   **Attention** : Le nom doit correspondre EXACTEMENT (insensible à la casse) au nom dans le fichier CSV.

### ✨ Fonctionnalité Avancée : Colonnes Dynamiques

C'est une fonctionnalité puissante qui permet d'ajouter des informations qui ne sont **pas dans le fichier**, mais qui dépendent du **nom du fichier**.

**Cas d'usage** : Les fichiers `ipf16.csv` sont pour le Pôle 1 (Agent), et `ipf36.csv` pour le Pôle 3 (Courtage), mais cette info n'est pas une colonne du fichier.

**Configuration (`reading_config.json`)** :

```json
"dynamic_columns": [
    {
        "pattern": "*16*",       // Si le nom du fichier contient "16"
        "columns": {
            "cdpole": "1"        // Alors ajouter colonne "cdpole" avec valeur "1"
        }
    },
    {
        "pattern": "*36*",       // Si le nom du fichier contient "36"
        "columns": {
            "cdpole": "3"        // Alors ajouter colonne "cdpole" avec valeur "3"
        }
    }
]
```

Le `Reader` applique ces règles à la volée lors de la lecture. C'est transparent pour le reste du pipeline.

---

## ⚙️ 4. Configuration Globale (`config.yml`)

Ce fichier gère l'infrastructure et l'exécution du pipeline. Il est essentiel pour séparer l'environnement (Dev/Prod) du code.

### Pourquoi est-il nécessaire ?

Il permet de déployer le même code sur différents environnements (Local, Recette, Prod) sans changer une seule ligne de Python. Seul ce fichier change.

### Sections Clés à Connaître

1.  **`datalake`** : Définit où sont les données.
    ```yaml
    datalake:
      data_root: "/ABR/P4D/..."  # Racine du datalake
      paths:
         bronze_monthly: "..."   # Modèle de chemin
    ```
    *Si les chemins Azure changent, modifiez ici.*

2.  **`components`** : Active/Désactive des parties du pipeline.
    ```yaml
    components:
      ptf_mvt:
        enabled: true   # Mettre false pour sauter cette étape
    ```
    *Utile pour exécution par défaut.*

3.  **`spark`** : Optimisation des performances.
    ```yaml
    spark:
      config:
        "spark.driver.memory": "4g"  # Augmentez si erreur "Out of Memory"
    ```

---

## 🚀 5. Tutoriel : Utiliser `main.py` Efficacement

Le script `main.py` est le point d'entrée unique. Il est conçu pour être simple mais flexible.

### Commande de Base (Exécution Standard)

Pour lancer le traitement normal pour une vision (mois) donnée :

```bash
python main.py --vision 202512
```

**Ce que ça fait** :
1.  Charge la configuration depuis `config/config.yml` (par défaut).
2.  Regarde la section `components` dans le fichier YAML.
3.  Exécute **tous** les composants marqués `enabled: true`.

### Exécuter un Seul Composant (Force)

Si vous voulez uniquement recalculer les capitaux sans refaire tout le reste (même si désactivé dans la config) :

```bash
python main.py --component capitaux --vision 202512
```

L'option `--component` **ignore** l'état `enabled` dans `config.yml`.
**Composants disponibles** : `ptf_mvt`, `capitaux`, `emissions`.

### Changer de Fichier de Configuration

Si vous voulez tester une configuration différente (ex: en local ou recette) :

```bash
python main.py --vision 202512 --config config/config_dev.yml
```

Cela permet d'avoir plusieurs fichiers `config.yml` pour différents environnements sans modifier le code.

### Mode Debug / Test

Pour tester sur un environnement local ou voir les logs en détail :

1.  Modifiez `config.yml` pour mettre `logging.level: "DEBUG"`.
2.  Lancez avec une vision de test.

### Enchaînement Intelligent et Priorités

1.  **Arguments CLI** (`--vision`) > **Variables d'env** (`PIPELINE_VISION`) > **Défaut Config** (`runtime.vision_`).
2.  Le `main.py` initialise **une seule session Spark** partagée pour tout le pipeline (gain de temps).
3.  Si un composant échoue, le pipeline s'arrête proprement et loggue l'erreur, mais ne bloque pas les composants précédents réussis.

---

## 🎓 Résumé pour le Développeur

| Je veux...                              | Fichier à modifier                                |
| --------------------------------------- | ------------------------------------------------- |
| Changer un chemin de fichier            | `config/config.yml`                               |
| Changer le format d'un fichier d'entrée | `config/reading_config.json`                      |
| Ajouter une colonne lue                 | `config/column_definitions.py`                    |
| Changer une règle de calcul métier      | `config/transformations/*.json`                   |
| Changer la mémoire Spark                | `config/config.yml`                               |
| Ajouter un nouveau flux                 | Créer un nouveau processor dans `src/processors/` |

---

*Ce document doit vivre avec le projet. Mettez-le à jour si vous ajoutez de nouvelles fonctionnalités de configuration.*
