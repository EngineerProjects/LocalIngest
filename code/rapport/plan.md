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

*Exemples détaillés de résolution en Annexe 4*

#### 4.5 Validation fonctionnelle

**4.5.1 Tests unitaires**
- Exécution sans erreur des 3 pipelines
- Logs complets et cohérents
- Écriture Parquet réussie pour toutes les sorties

**4.5.2 Comparaisons KPI**
- Correspondance avec études KPI SAS
- Cohérence des comptages (nombre de polices, primes totales)

**4.5.3 Tests de parité (en cours)**
- Méthodologie : 20 visions sur 2 périodes (N, N-1)
- Critères de succès définis (écart < 0.1%)

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
