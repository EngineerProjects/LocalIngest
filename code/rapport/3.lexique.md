# LEXIQUE

Ce lexique définit les termes techniques, acronymes métier et concepts utilisés dans ce rapport. Les définitions reflètent leur utilisation spécifique dans le contexte du projet de migration du datamart Construction.

---

## TERMES MÉTIER ASSURANCE

**AFN (Affaires Nouvelles)**  
Polices d'assurance souscrites ou renouvelées au cours de la période de référence. Indicateur clé de la production commerciale.

**AZ**  
Canal de distribution regroupant les Agents et le Courtage traditionnel. Se distingue d'AZEC par ses sources de données et sa logique métier spécifique.

**AZEC**  
Canal spécialisé dans le Courtage Construction (entreprises de construction). Utilise des règles de gestion et des sources de données différentes d'AZ.

**Coassurance**  
Partage d'un risque assuré entre plusieurs assureurs. La compagnie ne porte qu'une partie du risque total (exprimée en pourcentage).

**LCI (Limite Contractuelle d'Indemnité)**  
Montant maximum que l'assureur s'engage à verser en cas de sinistre, défini contractuellement. Capital crucial pour l'évaluation du risque.

**P&C (Property & Casualty)**  
Assurance des biens et responsabilités. Regroupe les marchés : Construction, Auto, Habitation, Santé, Épargne.

**PE (Perte d'Exploitation)**  
Capital assuré couvrant les pertes financières consécutives à un sinistre (arrêt d'activité, perte de chiffre d'affaires).

**PTF (Portefeuille)**  
Ensemble des polices d'assurance en vigueur à une date donnée. Indicateur de l'encours commercial.

**RD (Risque Direct)**  
Capital assuré couvrant les dommages matériels directs aux biens assurés (bâtiments, équipements).

**RES (Résiliations)**  
Polices d'assurance résiliées ou non renouvelées au cours de la période. Indicateur de l'attrition commerciale.

**RPC (Remises en Portefeuille Avec Modification de Prime)**  
Polices réintégrées au portefeuille après suspension, avec ajustement tarifaire.

**RPT (Remises en Portefeuille Sans Modification de Prime)**  
Polices réintégrées au portefeuille après suspension, sans changement de prime.

**SMP (Sinistre Maximum Possible)**  
Estimation du montant maximum qu'un sinistre pourrait coûter à l'assureur. Indicateur d'exposition au risque.

---

## TERMES TECHNIQUES DATA ENGINEERING

**Architecture Médaillon (Medallion Architecture)**  
Architecture moderne de traitement de données en trois couches :
- **Bronze** : Données brutes ingérées sans transformation
- **Silver** : Données nettoyées, transformées et enrichies métier
- **Gold** : Données consolidées, prêtes pour l'analyse et la Business Intelligence

**Azure**  
Plateforme cloud de Microsoft hébergeant l'infrastructure data d'Allianz (stockage ADLS, calcul Databricks).

**Databricks**  
Plateforme cloud unifiée pour le traitement des données basée sur Apache Spark. Environnement cible de l'architecture Python.

**Datamart**  
Base de données agrégée et structurée, spécialisée par domaine métier (Construction, Auto, etc.), destinée à l'analyse décisionnelle.

**Delta Lake**  
Format de stockage optimisé extension de Parquet, offrant les transactions ACID et la gestion de versions. Évolution prévue du projet.

**Parquet**  
Format de fichier binaire optimisé pour le stockage et le traitement de données volumineuses en colonnes (compression, performance).

**Pipeline**  
Chaîne automatisée de traitements de données : lecture → transformation → enrichissement → écriture. Le projet en comporte trois : PTF Mouvements, Capitaux, Émissions.

**PySpark**  
Interface Python d'Apache Spark. Framework de traitement distribué permettant de manipuler des volumes massifs de données en parallèle.

**SAS (Statistical Analysis System)**  
Langage et plateforme historique de traitement de données utilisés actuellement pour les datamarts. Technologie source de la migration.

**Vision**  
Période de référence pour l'extraction des données, au format YYYYMM (ex: 202512 = Décembre 2025). Permet le suivi historique des datamarts.

---

## ACRONYMES ENTREPRISE ALLIANZ

**ADC (Advanced Data & Climate)**  
Pôle d'Allianz France spécialisé en data management, expertises climatiques et modélisations. Équipe d'accueil du stage.

**CDPOLE**  
Code Pôle de distribution : "1" = Agents, "3" = Courtage. Dimension clé de segmentation commerciale.

**DIRCOM (Direction Commerciale)**  
Canal de distribution des polices : "AZ" ou "AZEC". Distingue les processus métier et les sources de données.

**ISIC (International Standard Industrial Classification)**  
Classification internationale des activités économiques. Utilisée pour la codification sectorielle des risques assurés.

**NAF (Nomenclature d'Activités Française)**  
Classification française des activités économiques (révisions 2003 et 2008). Enrichissement des données clients.

**P4D**  
Direction d'Allianz France regroupant plusieurs pôles dont Advanced Data & Climate. Département porteur du projet "SAS EXIST".

---

## CONCEPTS TECHNIQUES PROJET

**Configuration externalisée**  
Approche architecturale séparant les règles métier (fichiers JSON/YAML) du code Python. Facilite la maintenance et l'évolution.

**Indexation FFB (Fédération Française du Bâtiment)**  
Ajustement des capitaux assurés en fonction des indices d'évolution des coûts de construction. Spécifique au marché Construction.

**Parité fonctionnelle**  
Critère de validation garantissant que les résultats Python sont strictement identiques aux résultats SAS (0% d'écart sur les KPI).

**Processor**  
Module Python responsable d'un pipeline complet : lecture Bronze, transformations métier, enrichissements, écriture Silver/Gold.

**Projet "SAS EXIST"**  
Projet stratégique Allianz de modernisation de l'infrastructure data : migration des datamarts SAS vers Azure/PySpark pour réduire les coûts et améliorer les performances.

**Schéma harmonisé**  
Alignement des structures de données AZ et AZEC pour permettre leur consolidation dans la couche Gold (mapping de colonnes).

---

## ACRONYMES TECHNIQUES ADDITIONNELS

**ADLS (Azure Data Lake Storage)**  
Service de stockage Azure pour données volumineuses. Héberge les couches Bronze, Silver et Gold.

**CSV (Comma-Separated Values)**  
Format de fichier texte pour données tabulaires. Format source des données brutes en couche Bronze.

**IRD (Inventaire des Risques et Données)**  
Référentiel Allianz détaillant les caractéristiques techniques des risques assurés (travaux, dates, montants).

**KPI (Key Performance Indicator)**  
Indicateurs clés de performance. Utilisés pour valider la parité Python vs SAS (nombre de polices, sommes de primes, etc.).

**POO (Programmation Orientée Objet)**  
Paradigme de programmation Python basé sur des classes et héritages. Architecture retenue pour les processors.

---

**Note** : Les définitions sont adaptées au contexte spécifique du projet. Certains termes peuvent avoir d'autres acceptions dans des contextes différents.
