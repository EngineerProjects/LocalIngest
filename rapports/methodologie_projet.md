# Méthodologie du Projet - Migration SAS vers PySpark
## Rapport de Stage - Datamart Construction (Marché 6)

---

## 📋 Vue d'Ensemble

Ce document retrace la **méthodologie appliquée** durant le projet de migration du pipeline de données Construction, de SAS vers PySpark. Il détaille les étapes suivies, les choix effectués, les défis rencontrés et les solutions apportées.

**Durée du projet** : Novembre 2024 - Janvier 2025 (2 mois)  
**Équipe** : 1 stagiaire + 1 maître de stage  
**Périmètre** : 3 pipelines (PTF Mouvements, Capitaux, Émissions)

---

## 🎯 Démarche Globale

La migration a été conduite selon une approche **itérative et incrémentale**, structurée en 5 phases principales :

1. **Analyse et compréhension** du code SAS existant
2. **Conception de l'architecture** cible Python
3. **Recensement systématique** des règles métier et datasets
4. **Implémentation** des pipelines Python
5. **Validation et tests** de parité fonctionnelle

Chaque phase s'est appuyée sur les livrables de la phase précédente, garantissant une progression méthodique et tracée.

---

## Phase 1 : Analyse et Documentation du Code SAS Existant

### 1.1 Contexte et Objectifs

**Situation initiale** :
- Code SAS en production depuis plusieurs années
- 19 fichiers SAS (~15 000 lignes de code)
- Absence de documentation technique centralisée
- Macros SAS imbriquées et complexes
- Expertise SAS limitée dans l'équipe

**Objectif de la phase** :
Comprendre en profondeur la logique métier existante et documenter le fonctionnement actuel avant d'entreprendre la migration.

### 1.2 Démarche d'Analyse

**Étape 1 : Lecture systématique du code SAS**

J'ai commencé par analyser les fichiers SAS dans l'ordre d'exécution :

1. **Fichiers de run** (orchestration) :
   - `PTF_MVTS_RUN.sas` (204 lignes)
   - `CAPITAUX_RUN.sas` (211 lignes)
   - `EMISSIONS_RUN.sas` (308 lignes)

2. **Macros métier** (logique de traitement) :
   - `PTF_MVTS_AZ_MACRO.sas` (509 lignes)
   - `PTF_MVTS_AZEC_MACRO.sas` (490 lignes)
   - `CAPITAUX_AZ_MACRO.sas` (313 lignes)
   - `CAPITAUX_AZEC_MACRO.sas` (149 lignes)
   - `PTF_MVTS_CONSOLIDATION_MACRO.sas` (non disponible initialement)

3. **Fichiers utilitaires** :
   - `generiques_v4.sas` (fonctions génériques)
   - `indexation_v2.sas` (indexation des capitaux)
   - `CODIFICATION_ISIC_CONSTRUCTION.sas` (25 442 lignes !)
   - `REF_segmentation_azec.sas` (13 181 lignes)

**Étape 2 : Identification des flux de données**

Pour chaque pipeline, j'ai tracé :
- Les **sources de données** (LIBNAME SAS)
- Les **transformations appliquées** (filtres, calculs, jointures)
- Les **tables de sortie** (outputs CUBE)

**Exemple pour PTF Mouvements** :
```
Sources:
  - PTF16.IPF, PTF36.IPF (portfolio)
  - PTF16a.IPFM99, PTF36a.IPFM99 (CA produit 01099)
  - SEGMprdt.PRDPFA1/PRDPFA3 (segmentation)
  - PT_GEST.PTGST_* (points de gestion)
  - CLIENT1, CLIENT3 (données clients)

Transformations:
  - Filtres marché construction (CMARCH=6, CSEG=2)
  - Calcul AFN/RES/PTF/RPT/RPC
  - Extraction capitaux (14 champs → SMP, LCI, PE, RD)
  - Calcul expositions (YTD, GLI)
  - Enrichissements (IRD, ISIC, clients)

Outputs:
  - CUBE.MVT_CONST_PTF_{vision} (AZ)
  - CUBE.AZEC_PTF_{vision} (AZEC)
  - CUBE.AZ_AZEC_PTF_{vision} (consolidé)
```

**Étape 3 : Compréhension de la logique métier**

Points d'attention identifiés :
- **Logique AFN/RES différente** entre AZ et AZEC
- **Extraction capitaux** complexe (boucle sur 14 champs avec pattern matching)
- **Indexation** des capitaux (avec coefficients d'évolution)
- **Gestion des coassurances** (calcul cotisation à 100%)
- **Codes ISIC** (6 tables de mapping avec fallbacks successifs)

### 1.3 Livrables de la Phase 1

**Document 1 : Documentation fonctionnelle SAS** (`docs/SAS_DOCUMENTATION.md`)

J'ai créé une **documentation complète** du fonctionnement SAS en deux versions :

**Version synthétique** (pour l'équipe) :
- Vue d'ensemble des 3 pipelines
- Schémas de flux de données
- Tableaux récapitulatifs des transformations clés
- Glossaire des termes métier

**Version technique détaillée** (pour les développeurs curieux) :
- Ligne par ligne des transformations SAS
- Mapping des macros SAS
- Explication des calculs complexes (formules)
- Gestion des cas limites

**Extrait du sommaire** :
```markdown
# Documentation SAS - Datamart Construction

## 1. Vue d'Ensemble
   1.1 Architecture SAS actuelle
   1.2 Les 3 pipelines
   1.3 Flux de données

## 2. Pipeline PTF Mouvements
   2.1 Sources de données (AZ vs AZEC)
   2.2 Calcul des mouvements (AFN, RES, PTF)
   2.3 Extraction des capitaux
   2.4 Enrichissements (IRD, ISIC, clients)
   2.5 Consolidation AZ + AZEC

## 3. Pipeline Capitaux
   3.1 Extraction SMP/LCI avec indexation
   3.2 Calcul PE/RD
   3.3 Normalisation à 100%

## 4. Pipeline Émissions
   4.1 Connexion One BI
   4.2 Filtres et exclusions
   4.3 Calcul primes N vs X

## 5. Annexes
   5.1 Glossaire métier
   5.2 Mapping des libnames
   5.3 Liste des transformations
```

**Bénéfices apportés** :
- ✅ Compréhension partagée du fonctionnement SAS
- ✅ Documentation pérenne (même si SAS est arrêté)
- ✅ Base solide pour la migration Python
- ✅ Référence pour valider la parité fonctionnelle

### 1.4 Défis Rencontrés

**Défi 1 : Complexité du code SAS**
- Macros imbriquées sur plusieurs niveaux
- Variables globales utilisées sans documentation
- Logique conditionnelle complexe (visions historiques vs courantes)

**Solution** : Créer des schémas visuels et des exemples concrets pour chaque macro.

**Défi 2 : Manque de commentaires**
- Beaucoup de code sans explication
- Noms de variables cryptiques (`AH0`, `AMN0`, etc.)

**Solution** : Déduire la logique par analyse inverse + validation avec le maître de stage.

**Défi 3 : Code ISIC volumineux (25 000 lignes)**
- Impossible à analyser ligne par ligne
- Multiples tables de mapping imbriquées

**Solution** : Focus sur la logique globale (fallback strategy) plutôt que les détails.

---

## Phase 2 : Conception de l'Architecture Cible Python

### 2.1 Réunion de Cadrage avec le Maître de Stage

Suite à l'analyse SAS, j'ai présenté mes conclusions et proposé plusieurs options d'architecture :

**Option 1 : Réplication à l'identique**
- Reproduire la structure SAS en Python (1 script = 1 fichier SAS)
- ✅ Plus simple à valider
- ❌ Conserve les défauts de l'architecture SAS

**Option 2 : Architecture médaillon (RECOMMANDÉ)**
- 3 couches Bronze/Silver/Gold
- ✅ Moderne et scalable
- ✅ Séparation des responsabilités
- ❌ Plus de développement initial

**Option 3 : Architecture hybride**
- Bronze/Gold uniquement (sans Silver intermédiaire)
- ❌ Perd les avantages de traçabilité

**Décision validée** : **Architecture médaillon complète** (Option 2)

**Justification** :
- Standard de l'industrie (Databricks, AWS, Azure)
- Meilleure qualité et traçabilité des données
- Facilite les évolutions futures
- Permet de réutiliser les données Silver

### 2.2 Adaptation de l'Architecture Médaillon

Nous avons adapté l'architecture médaillon standard au contexte du projet :

**Bronze Layer** :
- Données brutes CSV (format source conservé)
- Partitionnement : `bronze/{YYYY}/{MM}/` et `bronze/ref/`
- **Pas de transformation** (lecture directe)

**Silver Layer** :
- Format **Parquet** (compression + performance)
- Transformations métier par pipeline :
  - AZ : `mvt_const_ptf_{vision}`
  - AZEC : `azec_ptf_{vision}`
  - Capitaux AZ/AZEC
  - Émissions
- Déduplication et filtres métier appliqués

**Gold Layer** :
- Consolidation finale (AZ + AZEC)
- Enrichissements complets (IRD, ISIC, clients)
- Prêt pour consommation BI
- Format Parquet optimisé

**Schéma validé** :
```
┌─────────┐
│ Sources │ → CSV bruts
└────┬────┘
     │
     ▼
┌─────────┐
│ BRONZE  │ → Ingestion (CSV)
└────┬────┘
     │ read_file_group()
     ▼
┌─────────┐
│ SILVER  │ → Transformations (Parquet)
│ Processors
└────┬────┘
     │
     ▼
┌─────────┐
│  GOLD   │ → Consolidation (Parquet)
│ Consolidation
└────┬────┘
     │
     ▼
┌─────────┐
│ BI/Apps │
└─────────┘
```

### 2.3 Choix Techniques

**Stack validée** :
- **PySpark 3.x** : Traitement distribué
- **Python 3.9+** : Langage principal
- **Parquet** : Format optimisé
- **JSON/YAML** : Configuration externalisée

**Principes de conception** :
1. **Configuration > Code** : Externaliser les règles métier
2. **Modularité** : Code réutilisable et testable
3. **Logging détaillé** : Traçabilité complète
4. **Gestion d'erreurs gracieuse** : Fallback pour données manquantes

---

## Phase 3 : Recensement Systématique

### 3.1 Objectif de la Phase

Avant de coder, créer deux **fichiers de référence Excel** pour :
1. Inventorier toutes les **règles de gestion métier**
2. Recenser tous les **datasets et leurs sources**

### 3.2 Fichier 1 : Inventaire des Règles de Gestion

**Format Excel** : `docs/REGLES_GESTION_CONSTRUCTION.xlsx`

**Colonnes** :
- **ID** : Identifiant unique (RG001, RG002, etc.)
- **Pipeline** : PTF_MVT / CAPITAUX / EMISSIONS
- **Domaine** : Mouvements / Capitaux / Filtres / Enrichissements
- **Règle** : Description de la règle métier
- **Source SAS** : Fichier + ligne(s) de code SAS
- **Implémentation Python** : Fichier + fonction Python
- **Statut** : TODO / EN COURS / FAIT / VALIDÉ

**Exemples extraits** :

| ID | Pipeline | Domaine | Règle | Source SAS | Impl. Python |
|----|----------|---------|-------|------------|--------------|
| RG001 | PTF_MVT | Filtres | Marché construction uniquement (CMARCH=6, CSEGT=2) | PTF_MVTS_AZ_MACRO.sas L47-48 | az_config['filters'] |
| RG012 | PTF_MVT | Mouvements | NBAFN: AFN si (DTDEB_AN ≤ dteffan ≤ DTFIN) ET (DTDEB_AN ≤ dttraan ≤ DTFIN) | PTF_MVTS_AZ_MACRO.sas L259-263 | calculate_movements() |
| RG025 | PTF_MVT | Capitaux | LCI si lbcapi contient "LCI GLOBAL DU CONTRAT" | PTF_MVTS_AZ_MACRO.sas L198-204 | extract_capitals() |
| RG078 | CAPITAUX | Indexation | Indexation capitaux avec indices FFB | CAPITAUX_AZ_MACRO.sas L127 | indexation_v2() |
| RG134 | PTF_MVT | ISIC | Fallback CDNAF2008 → CDNAF2003 → ACTPRIN | CODIFICATION_ISIC L... | assign_isic_codes() |

**Total recensé** : **~150 règles de gestion** réparties sur les 3 pipelines

**Utilisation** :
- Checklist pour l'implémentation
- Traçabilité SAS → Python
- Base pour tests de validation

### 3.3 Fichier 2 : Recensement des Datasets

**Format Excel** : `docs/DATASETS_SOURCES_CONSTRUCTION.xlsx`

**Colonnes** :
- **Dataset** : Nom du fichier ou table
- **Source SAS** : LIBNAME SAS (ex: PTF16.IPF)
- **Fichier SAS** : Où est-il utilisé (ex: PTF_MVTS_AZ_MACRO.sas)
- **Type** : Mensuel / Référentiel
- **Format** : CSV / Autre
- **Taille** : Estimation
- **File Group Python** : Nom dans reading_config.json
- **Disponible** : OUI / NON / À VALIDER
- **Criticité** : CRITIQUE / IMPORTANT / OPTIONNEL

**Exemples extraits** :

| Dataset | Source SAS | Fichier SAS | Type | File Group Python | Disponible |
|---------|------------|-------------|------|-------------------|------------|
| ipf16.csv | PTF16.IPF | PTF_MVTS_AZ_MACRO.sas L134 | Mensuel | ipf_az | OUI |
| ipf36.csv | PTF36.IPF | PTF_MVTS_AZ_MACRO.sas L148 | Mensuel | ipf_az | OUI |
| polic_cu.csv | POLIC_CU.POLIC_CU | PTF_MVTS_AZEC_MACRO.sas L80 | Référentiel | polic_cu_azec | OUI |
| cproduit.csv | AACPRTF.Cproduit | PTF_MVTS_AZ_MACRO.sas L414 | Référentiel | cproduit | ⚠️ NON |
| ird_risk_q45_*.csv | (Généré) | PTF_MVTS_CONSOLIDATION L158 | Mensuel | ird_risk_q45 | OUI |

**Total recensé** : **45 file groups** (datasets ou groupes de fichiers)

**Bénéfices** :
- Vision complète des dépendances
- Identification rapide des datasets manquants
- Base pour `reading_config.json`
- Validation de complétude avant dev

### 3.4 Défis de cette Phase

**Défi 1 : Datasets multiples pour même concept**
- SAS utilise PRDPFA1 (Pole 1) et PRDPFA3 (Pole 3) séparément
- Python peut les unifier → décision de garder les deux + version unifiée

**Défi 2 : Datasets manquants**
- `cproduit.csv`, `prdcap.csv`, `lob.csv` non disponibles dans le datalake
- → Marqués comme "À VALIDER" puis rendus optionnels en Python

**Défi 3 : Nomenclature incohérente**
- SAS : LIBNAME parfois versionné (PTGST_202501 vs PTGST)
- → Standardisation dans file groups Python

---

## Phase 4 : Implémentation Python

### 4.1 Structure du Projet

**Étape 1 : Mise en place du conteneur**

Création de l'arborescence projet :
```
new_python/
├── config/              # Configuration
├── src/                 # Code source
│   ├── processors/      # Pipelines
│   ├── orchestrators/   # Orchestration
│   └── reader.py        # Lecture données
├── utils/               # Utilitaires
│   ├── transformations/ # Fonctions métier
│   ├── helpers.py
│   └── logger.py
├── docs/                # Documentation
├── logs/                # Logs d'exécution
└── main.py              # Point d'entrée
```

**Étape 2 : Modules de base (Readers et Helpers)**

J'ai commencé par les **fondations** :

1. **BronzeReader** (`src/reader.py`) :
   - Lecture fichiers CSV depuis `bronze/`
   - Gestion partitionnement (monthly/ref)
   - Validation schémas
   - Gestion erreurs gracieuse

2. **Helpers** (`utils/helpers.py`) :
   - `compute_date_ranges()` : Calcul dates (DTFIN, DTDEB_AN, etc.)
   - `extract_year_month_int()` : Parsing vision
   - `write_to_layer()` : Écriture Parquet
   - `build_layer_path()` : Construction chemins

3. **Logger** (`utils/logger.py`) :
   - Logger custom avec niveaux (INFO/WARNING/ERROR/DEBUG)
   - Méthodes `.step()`, `.section()`, `.success()`
   - Output console + fichiers logs

4. **Configuration** (`config/`) :
   - `reading_config.json` : 45 file groups avec schémas
   - `schemas.py` : Schémas Spark (StructType)
   - `constants.py` : Constantes métier (POLE, DIRCOM, etc.)
   - `transformations/` : Règles métier JSON

**Tests des fondations** :
- Test lecture de chaque file group
- Validation schémas
- Test écriture Parquet
- → **Fondations validées avant de continuer**

### 4.2 Ordre d'Implémentation des Pipelines

**Choix de l'ordre** :

1. **PTF Mouvements** (le plus complexe)
   - Raison : Si on arrive à faire le plus dur d'abord, le reste sera plus facile
   - Contient tous les types de transformations
   - Permet de créer les utilitaires réutilisables

2. **Capitaux** (complexité moyenne)
   - Réutilise beaucoup d'utilitaires de PTF
   - Logique d'indexation spécifique

3. **Émissions** (le plus simple)
   - Logique linéaire (filtres → agrégations → enrichissement)
   - Pas de consolidation AZ/AZEC

**Avantage de cet ordre** :
- Création progressive des utilitaires
- Validation incrémentale
- Difficultés affrontées tôt dans le projet

### 4.3 Implémentation PTF Mouvements

**Sous-phases** :

**4.3.1 AZ Processor** (Agent + Courtage)

Fichier : `src/processors/ptf_mvt_processors/az_processor.py`

**Architecture** :
```python
class AZProcessor(BaseProcessor):
    def read(vision) → DataFrame        # Lecture ipf_az
    def transform(df, vision) → DataFrame  # 14 étapes de transformation
    def write(df, vision)                # Écriture silver
```

**14 étapes de transformation implémentées** :
```python
# STEP 0: Filtres métier (construction market)
# STEP 1: Rename columns (csegt → cseg, etc.)
# STEP 2: Initialize columns (0 values)
# STEP 3: Computed columns (tx, top_coass, coass, partcie)
# STEP 4: Metadata (vision, dircom, cdpole)
# STEP 5: Join IPFM99 (produit 01099)
# STEP 6: Extract capitals (SMP, LCI, PE, RD)
# STEP 7: Calculate premiums (primeto, top_lta)
# STEP 8: Calculate movements (AFN, RES, PTF, RPT, RPC)
# STEP 9: Calculate exposures (EXPO_YTD, EXPO_GLI)
# STEP 10: Calculate cotisation 100% and CA
# STEP 11: Business rules (TOP_AOP, anticipés)
# STEP 12: Data cleanup
# STEP 13: Enrich segmentation
# STEP 14: Deduplication
```

**Pattern utilisé** : Chaque étape est **indépendante** et **testable**

**Fonctions réutilisables créées** (dans `utils/transformations/`) :
- `apply_business_filters()` : Filtres config-driven
- `extract_capitals()` : Extraction capitaux par pattern matching
- `calculate_movements()` : Logique AFN/RES/PTF
- `calculate_exposures()` : Calculs YTD/GLI
- `rename_columns()` : Renommage batch

**4.3.2 AZEC Processor**

Fichier : `src/processors/ptf_mvt_processors/azec_processor.py`

**Spécificités AZEC** :
- Logique AFN/RES différente (produits avec gestion spéciale)
- 7 tables à joindre (capitaux, incendcu, rcentcu, etc.)
- Calcul CA depuis MULPROCU
- Gestion migration AZEC → IMS (ref_mig_azec_vs_ims)

**Pattern réutilisé** : Même structure read/transform/write

**4.3.3 Consolidation Processor**

Fichier : `src/processors/ptf_mvt_processors/consolidation_processor.py`

**Logique** :
1. Read AZ + AZEC depuis Silver
2. Harmonize schemas (renommage pour compatibilité)
3. Union AZ + AZEC
4. Enrichissements séquentiels :
   - IRD risk (Q46 → Q45 → QAN)
   - Client data (SIRET, SIREN, Euler)
   - ISIC codification (6 tables)
   - Special products (IPFM0024/63/99)
   - Business flags (Berlioz, Partenariat)
5. Write to Gold

**Défi technique** : Harmonisation schémas AZ ↔ AZEC
- Colonnes différentes (POLICE vs NOPOL, INTERMED vs NOINT, etc.)
- → Configuration JSON pour mapping automatique

### 4.4 Implémentation Capitaux

**Plus simple car réutilisation** :
- Même structure AZ + AZEC + Consolidation
- Fonctions d'extraction capitaux déjà créées
- Logique indexation isolée dans fonction dédiée

**Spécificité** : Indexation des capitaux
- Coefficients d'évolution par champ
- Calcul avec/sans indexation en parallèle

### 4.5 Implémentation Émissions

**Le plus simple** :
- Pas de consolidation AZ/AZEC
- Logique linéaire
- Réutilisation enrichissement segmentation

**Particularité** : Deux outputs (POL et POL_GARP)

### 4.6 Stratégies de Développement

**Approche itérative** :
1. Coder une transformation
2. Tester immédiatement
3. Logger les résultats
4. Valider avec échantillon de données
5. Passer à la transformation suivante

**Validation continue** :
- Logs détaillés à chaque étape
- Comptage lignes avant/après chaque transformation
- Vérification valeurs nulles
- Contrôle cohérence (ex: NBPTF ≥ NBAFN)

**Gestion erreurs** :
- Try/except sur enrichissements optionnels
- Fallback gracieux si données manquantes
- Logs WARNING (pas ERROR) pour données optionnelles

---

## Phase 5 : Validation et Tests

### 5.1 Tests Unitaires de Fonctionnement

**Objectif** : Valider que le code Python s'exécute sans erreurs

**Tests effectués** (en cours) :

| Pipeline | Vision Test | Statut | Temps Exécution | Commentaires |
|----------|-------------|--------|-----------------|--------------|
| PTF_MVT | 202509 | ✅ OK | ~X min | Aucune erreur |
| CAPITAUX | 202509 | ✅ OK | ~X min | Aucune erreur |
| ÉMISSIONS | 202509 | ✅ OK | ~X min | Aucune erreur |

**Validations techniques** :
- ✅ Lecture de toutes les sources Bronze
- ✅ Exécution sans exception Python
- ✅ Écriture Parquet en Silver/Gold
- ✅ Logs complets et cohérents

### 5.2 Tests de Parité Fonctionnelle (À VENIR)

**Prochaine étape** : Comparaison SAS vs Python

**Méthodologie prévue** :

**1. Sélection de 20 visions** :
- Vision courante : 202501
- Visions historiques : 202412, 202411, ..., 202301
- Couverture : 2 ans de données

**2. Extraction outputs SAS** :
- Tables CUBE.AZ_AZEC_PTF_*
- Tables CUBE.AZ_AZEC_CAPITAUX_*
- Tables CUBE.PRIMES_EMISES*

**3. Exécution Python** sur mêmes visions

**4. Comparaisons** :
- **Niveau macro** : Nombre de lignes (±1%)
- **Niveau KPIs** : Sommes des indicateurs (NBPTF, PRIMES_PTF, SMP_100, etc.)
- **Niveau détail** : Échantillon de 100 polices comparées champ par champ

**5. Critères de succès** :
- ✅ Nombre lignes : écart < 1%
- ✅ KPIs agrégés : écart < 0.01%
- ✅ Polices échantillon : 95%+ strictement identiques

**6. Actions si écarts** :
- Investigation ligne par ligne
- Correction code Python
- Re-test jusqu'à parité

### 5.3 Tests de Performance (À VENIR)

**Benchmarks prévus** :

**Environnement SAS** :
- Mainframe production
- [X] CPU / [Y] GB RAM

**Environnement Python** :
- Cluster Spark [config]
- [X] workers × [Y] cores

**Métriques** :
- Temps d'exécution par pipeline
- Consommation CPU/RAM/I-O
- Scalabilité (1 vision vs 12 visions)

---

## 🎓 Compétences Développées

### Compétences Techniques

**Langages et frameworks** :
- ✅ **PySpark** : DataFrames, SQL, transformations distribuées
- ✅ **Python** : POO, gestion fichiers, logging
- ✅ **SAS** : Lecture et compréhension de macros complexes
- ✅ **SQL** : Jointures, agrégations, window functions

**Architecture et design** :
- ✅ Architecture médaillon (Bronze/Silver/Gold)
- ✅ Design patterns (Factory, Strategy, Template Method)
- ✅ Séparation responsabilités (Processors, Orchestrators, Readers)
- ✅ Configuration externalisée (JSON/YAML)

**Outils et environnements** :
- ✅ Git (versioning, branches)
- ✅ Spark (local + cluster)
- ✅ Parquet (optimisation stockage)
- ✅ Excel (documentation et traçabilité)

### Compétences Métier

**Assurance Construction** :
- ✅ Compréhension des produits (AZ, AZEC, construction)
- ✅ Indicateurs métier (AFN, RES, PTF, SMP, LCI, etc.)
- ✅ Logique de coassurance et cession
- ✅ Codification ISIC et NAF

**Gestion de données** :
- ✅ Qualité des données (validation, déduplication)
- ✅ Traçabilité (logging, audit)
- ✅ Performance (Parquet, caching, broadcast)

### Compétences Transversales

**Méthodologie** :
- ✅ Analyse de code legacy
- ✅ Documentation technique
- ✅ Approche itérative et incrémentale
- ✅ Gestion de projet (phases, livrables)

**Communication** :
- ✅ Documentation claire (FR + EN)
- ✅ Présentation technique (PowerPoint)
- ✅ Reporting avancement

---

## 📊 Bilan et Enseignements

### Points Forts du Projet

**1. Documentation exhaustive du code SAS**
- Permet de comprendre un système complexe
- Sert de référence pérenne
- Facilite la validation

**2. Recensement systématique avant développement**
- Les fichiers Excel (règles + datasets) ont été essentiels
- Évite les oublis et découvertes tardives
- Permet suivi d'avancement précis

**3. Architecture médaillon bien adaptée**
- Séparation claire des responsabilités
- Facilite debugging (logs par couche)
- Évolutivité future

**4. Approche itérative**
- Validation continue
- Corrections rapides
- Risques maîtrisés

### Défis Rencontrés et Solutions

**Défi 1 : Complexité du code SAS**
- **Solution** : Documentation progressive + schémas visuels

**Défi 2 : Données de test incomplètes**
- **Solution** : Gestion gracieuse des données manquantes (fallback)

**Défi 3 : Harmonisation schémas AZ/AZEC**
- **Solution** : Configuration JSON pour mapping automatique

**Défi 4 : Logique ISIC très complexe**
- **Solution** : Modularisation en fonctions réutilisables

### Recommandations pour Futurs Projets

**Avant de coder** :
1. ✅ Documenter exhaustivement le code source
2. ✅ Recenser règles métier dans Excel/tableau
3. ✅ Inventorier tous les datasets (disponibilité !)
4. ✅ Valider l'architecture avec les parties prenantes

**Pendant le développement** :
1. ✅ Commencer par les fondations (readers, helpers)
2. ✅ Tester chaque module isolément
3. ✅ Logger abondamment
4. ✅ Valider avec petits échantillons avant full run

**Pour la validation** :
1. ✅ Prévoir temps suffisant pour tests de parité
2. ✅ Automatiser comparaisons SAS vs Python
3. ✅ Documenter les écarts acceptables
4. ✅ Impliquer les métiers dans validation

---

## 📅 Timeline Récapitulative

| Période | Phase | Activités Principales | Livrables |
|---------|-------|----------------------|-----------|
| **Semaine 1** (Nov) | **Intégration** | Rencontres équipe, formations Allianz (éthique, règles internes), familiarisation avec le datamart Construction | Accès et compréhension initiale |
| **Semaine 2-3** (Nov) | Analyse & Documentation SAS | Lecture code SAS, schémas flux, rédaction documentation (2 versions) | `SAS_DOCUMENTATION.md` |
| **Semaine 4** (Nov-Déc) | Conception & Recensement | Réunion architecture (validation médaillon), création fichiers Excel (règles + datasets) | Architecture validée + 2 fichiers Excel |
| **Semaine 5** (Déc) | Setup Projet | Arborescence, readers, helpers, configuration | Fondations Python (readers, helpers, config) |
| **Semaine 6** (Déc) | PTF Mouvements | Implémentation AZ → AZEC → Consolidation | 3 processors PTF |
| **Semaine 7** (Déc) | Capitaux & Émissions | Implémentation Capitaux (AZ/AZEC) + Émissions | 4 processors (Capitaux + Émissions) |
| **Semaine 8** (Déc-Jan) | Tests Unitaires | Exécution sans erreurs, validation logs | Logs de validation, pipelines fonctionnels |
| **Semaine 9** (Jan) | **Tests Parité** | Comparaison SAS vs Python (20 visions), benchmarks performance | ⏳ **EN COURS** |

---

**Document rédigé pour le rapport de stage**  
**Date** : Janvier 2025  
**Auteur** : [Votre nom]
