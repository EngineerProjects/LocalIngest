## **PLAN DÉTAILLÉ DU RAPPORT DE STAGE**

---

### **PAGES LIMINAIRES (non numérotées)**
- **Couverture** (obligatoire)
- **Résumé** (300 mots + mots-clés en FR/EN)
- **Remerciements**
- **Préambule** (facultatif - pour préciser la mission principale développée)
- **Lexique** (SAS, PySpark, AFN, RES, PTF, médaillon, Bronze/Silver/Gold, ISIC, AZEC, etc.)
- **Sommaire**

---

### **INTRODUCTION** (1-2 pages)
*Justification de l'étude pour Allianz*

- **Accroche** : Contexte de transformation digitale dans l'assurance
- **Mise en situation** : Modernisation de l'infrastructure data chez Allianz (migration SAS → Azure)
- **Position du problème** : Nécessité de migrer les datamarts critiques vers des technologies cloud modernes
- **Annonce du plan**

---

### **1. CADRE DE L'ÉTUDE** (2-3 pages)

#### 1.1 Présentation d'Allianz France
- Positionnement et activités
- Focus sur la direction P4D - ADC

#### 1.2 Le pôle Advanced Data & Climate
- Missions : data management, expertise climat, modélisations
- Positionnement dans la direction P4D
- Équipes et domaines d'intervention

#### 1.3 L'équipe d'accueil
- Rôle : mise à disposition des datamarts et projets transverses
- Positionnement dans le pôle

#### 1.4 Contexte stratégique et enjeux de la migration
- Importance des datamarts dans l'analyse assurantielle
- Contexte du projet "SAS EXIST"
- Enjeux de la migration SAS vers Azure

#### 1.5 Problématique et objectifs du stage

**1.5.1 Problématique**  
Comment migrer le datamart Construction de SAS vers PySpark tout en garantissant :
- La parité fonctionnelle (résultats identiques)
- Une architecture moderne et évolutive
- Une documentation complète
- Une maintenabilité accrue

**1.5.2 Enjeux stratégiques**
- Réduction des coûts (licences SAS)
- Amélioration des performances
- Scalabilité vers d'autres datamarts
- Autonomie de l'équipe

**1.5.3 Objectifs du stage**
- Créer le premier prototype de migration (marché Construction)
- Valider la faisabilité technique
- Documenter la méthodologie pour les autres marchés
- Établir les bases d'une architecture cible réutilisable

---

### **2. CONTEXTE TECHNIQUE ET ÉTAT DE L'ART** (6-8 pages)

#### 2.1 L'existant : le datamart Construction en SAS

**2.1.1 Rôle des datamarts dans l'analyse assurantielle**
- Fonction des datamarts P&C
- Le marché Construction (marché 6) : spécificités métier

**2.1.2 Architecture actuelle**
- Serveurs SAS et processus batch
- Flux de données et transformations
- Structure des 19 fichiers SAS (~15 000 lignes)

**2.1.3 Limites de l'architecture SAS**
- Coûts des licences
- Performance des serveurs vieillissants
- Manque de scalabilité
- Expertise SAS limitée dans l'équipe

#### 2.2 État de l'art

**2.2.1 Technologies de migration**
- **PySpark** : caractéristiques, avantages pour le traitement distribué
- **Azure Databricks** : plateforme cible, écosystème cloud
- **Formats de stockage modernes** : Parquet, Delta Lake

**2.2.2 Architectures data modernes**
- Architecture médaillon (Bronze/Silver/Gold)
- Comparaison avec l'architecture SAS existante
- Standards de l'industrie (Databricks, AWS, Azure)

**2.2.3 Revue bibliographique**
- Migrations SAS vers Python dans l'assurance
- Retours d'expérience et bonnes pratiques

---

### **3. MÉTHODOLOGIE ET MOYENS MIS EN ŒUVRE** (3-4 pages)

#### 3.1 Méthodologie globale
- Approche itérative et incrémentale
- Schéma synoptique des 5 phases du projet

#### 3.2 Déroulement du projet

**3.2.1 Phase 1 : Analyse et documentation du code SAS**
- Lecture systématique des fichiers SAS (19 fichiers, ~15 000 lignes)
- Identification des flux de données
- Traçage des transformations métier
- **Livrables** : Documentation SAS complète + schémas de la structure (2 versions)

**3.2.2 Phase 2 : Conception de l'architecture cible**
- Réunion de cadrage avec le maître de stage
- Comparaison des options d'architecture
- **Choix validé** : Architecture médaillon complète
- Justification du choix (séparation des responsabilités, standard industrie)

**3.2.3 Phase 3 : Recensement systématique**
- Inventaire des règles métier (72+ règles)
- Catalogue des datasets (tableaux Excel)
- Mapping des transformations SAS → Python

**3.2.4 Phase 4 : Implémentation Python**
- **Fondations** : Structure du projet (readers, helpers, config), logging, gestion de configuration (JSON/YAML)
- **Développement des pipelines** : PTF Mouvements (AZ, AZEC, consolidation), Capitaux, Émissions
- **Stratégie de développement** : Approche itérative (coder → tester → valider), tests avec données artificielles puis réelles

**3.2.5 Phase 5 : Validation**
- Tests unitaires de fonctionnement
- Tests de parité fonctionnelle prévus (20 visions sur 2 ans)
- Méthodologie de comparaison SAS vs Python

#### 3.3 Outils et technologies
- PySpark, Python (POO)
- Git (versioning)
- Azure (stockage ADLS)
- Parquet / Delta Lake
- Excel (documentation)

---

### **4. PRÉSENTATION ET ANALYSE DES RÉSULTATS** (12-18 pages)

#### 4.1 Architecture finale implémentée

**4.1.1 Vue d'ensemble**
- Schéma de l'architecture médaillon Bronze → Silver → Gold
- Comparaison avec l'architecture SAS
- Flux de données et points de transformation

**4.1.2 Couche Bronze**
- Ingestion des données CSV brutes
- Partitionnement temporel (année/mois)
- Conservation sans transformation

**4.1.3 Couche Silver**
- Transformations métier par pipeline
- Format Parquet optimisé
- Déduplication et filtres métier

**4.1.4 Couche Gold**
- Consolidation finale (AZ + AZEC)
- Enrichissements complets (IRD, ISIC, clients)
- Données prêtes pour BI

#### 4.2 Livrables produits

**4.2.1 Documentation**
- Documentation fonctionnelle SAS (synthétique + détaillée)
- Fichiers Excel de recensement (règles + datasets)
- Schémas d'architecture validés

**4.2.2 Code Python**
- Structure du projet (arborescence complète)
- 3 processors (PTF Mouvements, Capitaux, Émissions)
- Modules utilitaires (readers, helpers)
- Configuration externalisée (JSON/YAML)

#### 4.3 Résultats par pipeline

**4.3.1 Pipeline PTF Mouvements**
- Calculs AFN/RES/PTF/RPT/RPC
- Extraction des capitaux (14 champs : MTCA, LBCA, etc.)
- Enrichissements séquentiels (NAF, PE/RD, IRD, ISIC)
- Consolidation AZ + AZEC
- **Résultats** : Exécution réussie, logs cohérents, fichiers Parquet générés

**4.3.2 Pipeline Capitaux**
- Extraction SMP/LCI avec indexation
- Calcul PE/RD
- Normalisation à 100%
- **Résultats** : Fonctionnel, réutilisation code PTF

**4.3.3 Pipeline Émissions**
- Filtres et exclusions métier
- Calcul primes N vs X
- **Résultats** : Pipeline le plus simple, deux outputs distincts

#### 4.4 Défis techniques rencontrés et solutions

Tableau synthétique des principaux défis :

| Défi                              | Cause                                                   | Solution                                               | Impact   |
| --------------------------------- | ------------------------------------------------------- | ------------------------------------------------------ | -------- |
| **Gestion des valeurs nulles**    | SAS utilise "." pour null numérique, "" pour null texte | Standardisation avec composants Spark natifs           | Critique |
| **Formats de dates hétérogènes**  | Dates européennes ET américaines dans les fichiers      | Uniformisation des formats en amont (Bronze)           | Élevé    |
| **Harmonisation schémas AZ/AZEC** | Noms de colonnes différents (POLICE vs NOPOL, etc.)     | Configuration JSON pour mapping automatique            | Élevé    |
| **Complexité code ISIC**          | 6 tables de mapping avec fallbacks (~25 000 lignes SAS) | Modularisation en fonctions réutilisables              | Élevé    |
| **Calculs retournant zéro**       | Filtres JSON ambigus, formats de dates incorrects       | Réécriture filtres en PySpark pur, normalisation dates | Critique |

**4.4.1 Défis Critiques Résolus - Exemples Détaillés**

| Défi Technique                    | Description                                                                                             | Solution Implémentée                                                      | Fichier Code                       | Impact Quantifié                                                 |
| --------------------------------- | ------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------- | ---------------------------------- | ---------------------------------------------------------------- |
| **CDPOLE NULL pour AZEC**         | 16,124 lignes AZEC sans colonne `cdpole` - SAS hardcode `"3"` dans consolidation                        | Ajout `df.withColumn('cdpole', lit('3'))`                                 | `azec_processor.py` L106           | **Critique**: cdpole=3 passe de 38,569 → 54,693 (match 100% SAS) |
| **DTECHANN format Integer MMJJ**  | Colonne `dtechann` en IntegerType (1231 = 31 déc) - `month()` Spark attend DateType                     | Extraction arithmétique: `MOIS = dtechann / 100`, `JOUR = dtechann % 100` | `indexation.py` L122-130           | **Critique**: Indexation 14 champs capitaux (248k lignes)        |
| **Config EMISSIONS dict vs list** | `excluded_intermediaries` structure `{"description":..., "values":[...]}` - Spark literal HashMap error | Extraction `.get('values', [])` au lieu d'utilisation directe dict        | `emissions_operations.py` L151-157 | **Élevé**: 6 configs (intermediaries, guarantees, products...)   |
| **IRD Format SAS DATETIME**       | Dates IRD en format SAS `'14MAY1991:00:00:00'` - `to_date()` Spark incompatible                         | Helper `parse_sas_datetime()` extraction `dt_part.split(':')[0]`          | `risk_enrichment.py`               | **Élevé**: Enrichissement IRD 3 tables (Q46, Q45, QAN)           |

**Exemple de résolution : CDPOLE NULL**
- **Diagnostic** : Distribution cdpole montrait 16,124 NULL au lieu de 0
- **Analyse SAS** : `PTF_MVTS_CONSOLIDATION_MACRO.sas` L74 : `"AZEC" AS DIRCOM, "3" AS CDPOLE`  
- **Cause** : AZEC processor créait `dircom` mais pas `cdpole`
- **Solution** : 1 ligne ajoutée après L104 : `df = df.withColumn('cdpole', lit('3'))`
- **Validation** : Python match 100% SAS (cdpole=1: 32,319 | cdpole=3: 54,693)

*Exemples complets de code et diagnostics en Annexe 4*


#### 4.5 Validation fonctionnelle

**4.5.1 Tests Unitaires - Vision 202512**

Résultats d'exécution pour la vision de test (Décembre 2025) :

**Pipeline PTF Mouvements** :
- **Inputs** : 70,907 lignes (AZ Agent + Courtage) + 16,124 lignes (AZEC)
- **Output Gold** : 87,012 lignes consolidées  
- **Fichiers générés** : `mvt_ptf_202512.parquet` (23.4 MB format Parquet optimisé)
- **Temps d'exécution** : ~8 minutes (mesuré avec timer `main.py`)
- **Statut** : ✅ Exécution sans erreur, logs cohérents, 14 étapes de transformation réussies

**Pipeline CAPITAUX** :
- **Inputs** : 242,927 lignes (AZ) + 9,588 lignes (AZEC)  
- **Output Gold** : 248,438 lignes avec indexation  
- **Champs indexés** : 14 capitaux (SMP, LCI, PE/RD, LBCA, MTCA...)
- **Temps d'exécution** : ~5 minutes
- **Statut** : ✅ Indexation FFB appliquée, normalisation 100% réussie

**Pipeline ÉMISSIONS** :
- **Input One BI** : 1,173,238 lignes brutes
- **Après filtres** : 273,796 lignes (marché construction cd_marche='6')
- **Output attendu** : Primes par police (POL) + par garantie (POL_GARP)
- **Temps d'exécution** : ~3 minutes
- **Statut** : ⚠️ Pipeline fonctionnel - Diagnostic révélé: fichier référence contient uniquement données 2026 (nécessite fichiers mensuels)

**4.5.2 Validation Qualité Données**

**Distribution CDPOLE (exemple correction critique)** :

| cdpole       | **AVANT Correction** | **APRÈS Correction** | **SAS Attendu** | Match |
| ------------ | -------------------- | -------------------- | --------------- | ----- |
| NULL         | 16,124               | 0                    | 0               | ✅     |
| 1 (Agent)    | 32,319               | 32,319               | 32,319          | ✅     |
| 3 (Courtage) | 38,569               | 54,693               | 54,693          | ✅     |
| **Total**    | **87,012**           | **87,012**           | **87,012**      | ✅     |

Écart résolu : 16,124 lignes AZEC récupérées (passage de NULL → cdpole='3')

**4.5.3 Comparaisons KPI avec SAS**

Échantillon de validations effectuées (correspondance avec études KPI SAS) :

| Indicateur               | Pipeline | Python  | SAS     | Écart | Statut |
| ------------------------ | -------- | ------- | ------- | ----- | ------ |
| **Nombre total polices** | PTF_MVT  | 87,012  | 87,012  | 0.00% | ✅      |
| **Distribution Pole 1**  | PTF_MVT  | 32,319  | 32,319  | 0.00% | ✅      |
| **Distribution Pole 3**  | PTF_MVT  | 54,693  | 54,693  | 0.00% | ✅      |
| **Lignes CAPITAUX AZ**   | CAPITAUX | 242,927 | 242,927 | 0.00% | ✅      |
| **Lignes CAPITAUX Gold** | CAPITAUX | 248,438 | 248,438 | 0.00% | ✅      |

*Note* : Validation KPI détaillée par indicateur métier (NBPTF, NBAFN, NBRES, PRIMES_PTF, etc.) documentée dans `11_analyse_ecarts_kpi_python_vs_sas.md`

**4.5.4 Tests de Parité Fonctionnelle (en cours)**

**Méthodologie prévue** :
- **Périmètre** : 20 visions sur 2 années (N-1, N) : 202501, 202412, 202411, ..., 202301
- **Critères de succès** :
  - Nombre de lignes : écart < 1%
  - KPIs agrégés (sommes NBPTF, PRIMES_PTF, SMP_100...) : écart < 0.1%
  - Polices échantillon (100 polices) : 95%+ strictement identiques champ par champ
- **Calendrier** : Finalisation prévue Q1 2026

**Résultats préliminaires** :
- ✅ Vision 202512 : Validation structurelle réussie (comptages match 100%)
- ⏳ Visions historiques : Tests en attente disponibilité données SAS complètes


---

### **5. DISCUSSION ET PERSPECTIVES** (5-6 pages)

#### 5.1 Analyse critique du projet

**5.1.1 Réussites et points forts**
- **Documentation exhaustive** : Référence pérenne pour l'équipe, facilite les futures migrations, base solide pour validation
- **Méthodologie rigoureuse** : Recensement systématique avant développement, approche itérative efficace, validation continue
- **Architecture moderne et évolutive** : Séparation des responsabilités (Bronze/Silver/Gold), facilite debugging et maintenance, standard de l'industrie
- **Code modulaire et réutilisable** : Design patterns appliqués, configuration externalisée, facilite extension à d'autres marchés
- **Logging abondant** : Aide précieuse au debugging et à la traçabilité

**5.1.2 Difficultés et améliorations possibles**
- **Absence de documentation SAS initiale** : Temps important d'analyse, risque d'interprétation
- **Disponibilité des données de test** : Retard dans la validation réelle, nécessité de générer données artificielles
- **Complexité du code legacy** : Macros SAS imbriquées, variables globales non documentées
- **Améliorations possibles** : Tests unitaires automatisés plus tôt, validation incrémentale par module, documentation code au fil de l'eau

#### 5.2 Comparaison avec l'état de l'art
- Alignement avec bonnes pratiques Databricks
- Conformité aux standards Azure
- Approche comparable à d'autres projets de migration dans l'assurance

#### 5.3 Implications pour Allianz

**5.3.1 Gains attendus**
- **Réduction des coûts** : Économies sur licences SAS (estimation)
- **Amélioration des performances** : Traitement distribué, optimisation Spark
- **Scalabilité** : Infrastructure cloud élastique

**5.3.2 Défis identifiés**
- Temps de migration pour les 4 autres marchés
- Formation de l'équipe à PySpark
- Maintien double environnement pendant la transition

#### 5.4 Perspectives et recommandations

**5.4.1 Évolutions techniques**
- **Court terme** : Finaliser tests de parité (20 visions), benchmarks de performance, validation métier avec utilisateurs finaux
- **Moyen terme** : Extension à d'autres marchés (Auto, Habitation, etc.), migration incrémentale des autres datamarts, mise en production pilote
- **Long terme** : Arrêt complet de SAS, migration vers Delta Lake (évolution Parquet), industrialisation des pipelines, monitoring et alerting avancés

**5.4.2 Recommandations pour futures migrations**
- Reproduire l'approche méthodologique (recensement → conception → implémentation)
- Capitaliser sur la documentation et le code comme templates
- Former l'équipe à l'architecture médaillon
- Prévoir des tests de parité dès le début du développement

---

### **CONCLUSION** (1-2 pages)
- Synthèse du travail accompli
- Réponse à la problématique initiale
- Apports pour Allianz (réduction coûts, modernisation) et pour la formation (compétences data engineering)
- Perspective principale : extension aux autres marchés et généralisation de l'approche

---

### **RÉFÉRENCES BIBLIOGRAPHIQUES**
- Documentation Databricks (architecture médaillon)
- Documentation PySpark/Apache Spark
- Documentation Microsoft Azure (ADLS, Databricks)
- Articles sur migrations SAS → Python dans l'assurance
- Documentation interne Allianz
- Normes et standards (si applicable)

---

### **ANNEXES** (si nécessaire)

**Annexe 1** : Organigramme détaillé Allianz / P4D / Advanced Data & Climate

**Annexe 2** : Schémas détaillés des flux de données (SAS vs PySpark)

**Annexe 3** : Extraits de code SAS significatifs avec annotations

**Annexe 4** : Exemples détaillés de résolution de défis techniques

**Annexe 5** : Structure détaillée du projet Python (arborescence complète)

**Annexe 6** : Exemples de logs de validation

**Annexe 7** : Tableaux Excel de recensement (extraits représentatifs)

**Annexe 8** : Glossaire métier assurance construction détaillé

---

### **TABLES**
- **Table des figures**
- **Table des tableaux**
- **Liste des annexes**
