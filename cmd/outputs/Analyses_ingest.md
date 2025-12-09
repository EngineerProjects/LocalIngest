# Project Summary
## Repositories
- /home/amiche/Downloads/codes/Analyses
Branch: main
Files analyzed: 10
Estimated tokens: 7.2k
Analysis time: 0.13 seconds

# Directory Structure

```
└── Analyses
    ├── 1_PTF
    │   ├──  PTF_MVTS_CONSOLIDATION_MACRO.md
    │   ├── 1.PTF_MVTS_RUN.mmd
    │   ├── CODIFICATION_ISIC_CONSTRUCTION.md
    │   ├── PTF_MVTS_AZEC_MACRO.md
    │   ├── PTF_MVTS_AZ_MACRO.md
    │   └── REF_segmentation_azec.md
    ├── 2_capitaux
    │   ├── CAPITAUX_RUN.md
    │   └── indexation_v2.md
    ├── Analyse_global.md
    └── global.mmd
```

# Files Content

## 1_PTF/ PTF_MVTS_CONSOLIDATION_MACRO.md

```md
## PTF_MVTS_CONSOLIDATION_MACRO.sas - %consolidation_az_azec_mvt_ptf

### **Rôle et Objectif**
Cette macro **consolide les données AZ et AZEC** en une seule table finale harmonisée et enrichie avec les données de risque. C'est l'étape finale qui produit la table cube de sortie.

### **Tables Sources et Flux**

#### **INPUTS**
- **MVT_CONST_PTF{vision}** : Table AZ traitée par `%az_mvt_ptf`
- **AZEC_PTF{vision}** : Table AZEC traitée par `%azec_mvt_ptf`
- **cube.ird_risk_q46/q45/qan_{vision}** : Tables de données risque DataRisk
- **Tables de référence** : Client, destination, ISIC

#### **OUTPUT**
- **CUBE.MVT_PTF{vision}** : Table finale consolidée prête pour les analyses

### **Traitement Principal en 4 Étapes**

#### **Étape 1 : Union des Données AZ + AZEC**
**Logique conditionnelle** : Si `&vision >= 201211` → Union AZ + AZEC, sinon → AZ uniquement.
**Harmonisation des structures** : Mapping des champs avec noms différents entre AZ et AZEC.
**Exemples de mapping** :
- `NOPOL` (AZ) ↔ `POLICE` (AZEC) → `NOPOL` final
- `DIRCOM` = "AZ" pour Agent/Courtage, "AZEC" pour AZEC historique
- `CDNATP` (AZ) ↔ `CDNATP` (AZEC) → harmonisé

#### **Étape 2 : Enrichissement avec Données Risque DataRisk**
**Gestion conditionnelle** selon vision :
- **Vision ≥ 202210** : Utilise tables dynamiques `cube.ird_risk_{type}_{vision}`
- **Vision < 202210** : Utilise tables figées `RISK_REF.ird_risk_{type}_202210`

**3 types de tables risque enrichies** :
- **Q46** : Données chantiers avec `DTOUCHAN`, `DTRECTRX`, `DTREFFIN`
- **Q45** : Données complémentaires chantiers 
- **QAN** : Données d'analyse avec `DTRCPPR`

**Logique d'enrichissement** : Si champ manquant dans données principales → complété par données risque.

#### **Étape 3 : Corrections et Cohérence des Dates**
**Correction dates manquantes** : Si `DTRCPPR` manquant et `DTREFFIN` présent → `DTRCPPR = DTREFFIN`.
**Enrichissement client** : Ajout `CDSIRET`/`CDSIREN` depuis tables Client1/Client3.
**Note Euler** : Jointure avec `binsee.histo_note_risque` pour scoring risque.

#### **Étape 4 : Finalisation et Classification**
**Classification destination chantiers** : Si `segment2="Chantiers"` et `DESTINAT` manquant :
- **Analyse regex** sur `DSTCSC` et `LBNATTRV` (HABIT, LOG, LGT, MAIS, APPA, VILLA, INDIV) → `DESTINAT = "Habitation"`
- **Codes destination** ('01','02','03','04','22') → `DESTINAT = "Habitation"`
- **Défaut** → `DESTINAT = "Autres"`

**Enrichissement activités** : Récupération codes activités depuis tables spécialisées `IPFSPE` (IPFM0024, IPFM99).
**Application classification ISIC** : Appel final à `%code_isic_construction` sur la table consolidée.
**Mapping ISIC Global** : Transformation codes ISIC locaux vers codes ISIC globaux standardisés.

### **Règles de Gestion Importantes**

**RG-CONSOL-01 : Gestion de l'Historique**
- Vision < 201211 : Données AZ uniquement (pas d'AZEC)
- Vision ≥ 201211 : Consolidation AZ + AZEC

**RG-CONSOL-02 : Priorisation des Sources de Données**
- **Données principales** (AZ/AZEC) prioritaires
- **Données risque** utilisées pour combler les manques
- **Tables spécialisées** pour enrichissements spécifiques

**RG-CONSOL-03 : Classification Destination Automatique**
- **47 règles regex** pour classification automatique des chantiers
- **Codes normalisés** pour habitation vs autres
- **Défaut sécurisé** vers "Autres" si aucun mapping

**RG-CONSOL-04 : Harmonisation des Codes**
- **Nettoyage codes NAF** : Suppression codes factices ('00', '0000Z', '9999')
- **Standardisation ISIC** : Corrections hardcodées pour codes spécifiques
- **Format final** : Structure uniforme pour export cube

### **Usage dans l'Architecture**
**Dernière étape** de PTF_MVTS_RUN.sas. Produit la table finale `CUBE.MVT_PTF{vision}` utilisée par les applications métier et les rapports de pilotage Construction.

**Impact critique** : Sans cette consolidation, impossible d'avoir une vision unifiée du portefeuille Construction AZ+AZEC avec enrichissements risque et classifications métier.
```

## 1_PTF/1.PTF_MVTS_RUN.mmd

```mmd
flowchart TD
    A[Start: %macro run_ptf_mvts] --> B[Configuration TCP - options commid=tcp - serveur=STP3 7013]
    B --> C[Connexion STP3 - signon noscript remote=serveur]
    C --> D[Extraction variables temporelles - annee et mois depuis vision]
    D --> E[Calcul variables système - data _null_: AH0, ASYS, MSYS]
    
    E --> F{annee = ASYS and mois = MSYS ?}
    
    F -->|YES| G[Vision EN COURS - LIBNAME PTF16 IPFE16 - LIBNAME PTF36 IPFE36 - LIBNAME PTF16a IPFE1SPE - LIBNAME PTF36a IPF3SPE]
    F -->|NO| H[Vision HISTORIQUE - LIBNAME PTF16 IPFE16.G08AMMD.V00 - LIBNAME PTF36 IPFE36.G08AMMD.V00 - LIBNAME PTF16a IPFE1SPE - LIBNAME PTF36a IPF3SPE]
    
    G --> I[Définition librairies communes - segmprdt, PRDCAP, CLIENT1, CLIENT3]
    H --> I
    
    I --> J[Définition librairies SAS - Dest, Ref, MIG_AZEC, PT_GEST, etc.]
    J --> K[Création répertoire sortie - mkdir /Output/vision - LIBNAME CUBE]
    K --> L[Inclusion macros utilitaires - generiques_v4.sas - CODIFICATION_ISIC_CONSTRUCTION.sas]
    
    L --> M{vision < année courante - 5 ans ?}
    
    M -->|YES| N{annee ≤ 2014 ?}
    M -->|NO| O{vision = année courante - 5 ans ?}
    
    N -->|YES| P[alloc_spe_v3 pour 2015,12,IPFSPE]
    N -->|NO| Q[alloc_spe_v3 pour annee,12,IPFSPE]
    
    O -->|YES| R{mois vision < mois courant ?}
    O -->|NO| S[alloc_spe_v3 pour annee,mois,IPFSPE]
    
    R -->|YES| T[alloc_spe_v3 pour annee-1,12,IPFSPE]
    R -->|NO| U[alloc_spe_v3 pour annee,mois,IPFSPE]
    
    P --> V{vision entre 201211 et 202008 ?}
    Q --> V
    S --> V
    T --> V
    U --> V
    
    V -->|YES| W[alloc_azec_v3 pour annee,mois]
    V -->|NO| X[Calcul dates de référence - data _null_: DTFIN, DTOBS, etc.]
    
    W --> X
    X --> Y[Exécution macro AZ - az_mvt_ptf]
    Y --> Z[Inclusion REF_segmentation_azec.sas]
    Z --> AA[Exécution macro AZEC - azec_mvt_ptf]
    AA --> BB[Consolidation finale - consolidation_az_azec_mvt_ptf]
    BB --> CC[End]

    style A fill:#e1f5fe
    style CC fill:#c8e6c9
    style F fill:#fff3e0
    style M fill:#fff3e0
    style N fill:#fff3e0
    style O fill:#fff3e0
    style R fill:#fff3e0
    style V fill:#fff3e0
```

## 1_PTF/CODIFICATION_ISIC_CONSTRUCTION.md

```md
## CODIFICATION_ISIC_CONSTRUCTION.sas

### **Rôle et Objectif**
Macro `%code_isic_construction(table_source, vision)` qui **enrichit une table existante** avec codes ISIC et Hazard Grades pour le marché Construction.

### **Tables Sources et Flux de Données**

#### **INPUT**
- **`table_source`** : Table à enrichir (ex: données portefeuille, capitaux)
- **Champs obligatoires** : `CMARCH`, `CSEG`, `CSSSEG`, `CDPROD`, `ACTPRIN`, `DSTCSC`, `NOPOL`

#### **TABLES DE RÉFÉRENCE** (consultées)
1. **NAF_2008.IRD_SUIVI_ENGAGEMENTS_&vision** (si vision ≥ 202103)
   - **Mapping** : NAF 2008 par police/produit
2. **ISIC_CST.MAPPING_ISIC_CONST_ACT_&vision** 
   - **Mapping** : Activités → codes ISIC (contrats Réalisations)
3. **ISIC_CST.MAPPING_ISIC_CONST_CHT_&vision**
   - **Mapping** : Destinations → codes ISIC (contrats Chantier)  
4. **REF_ISIC.MAPPING_CDNAF2003_ISIC_&vision**
   - **Mapping** : NAF 2003 → ISIC
5. **REF_ISIC.MAPPING_CDNAF2008_ISIC_&vision**  
   - **Mapping** : NAF 2008 → ISIC
6. **REF_ISIC.table_isic_tre_naf_&vision**
   - **Mapping** : ISIC → Hazard Grades (7 types de risques)

#### **OUTPUT**
- **Table source enrichie** avec nouvelles colonnes :
  - `CDNAF2008`, `ISIC_CODE_SUI`, `DESTINAT_ISIC` 
  - `ISIC_CODE`, `ORIGINE_ISIC`
  - `HAZARD_GRADES_*` (FIRE, BI, RCA, RCE, TRC, RCD, DO)

### **Logique Principale**

#### **Flux de Traitement**
1. **Récupération NAF 2008** depuis suivi des engagements (si vision récente)
2. **Application hiérarchie ISIC** : NAF08-PTF → NAF03-PTF → NAF03-CLI → NAF08-CLI
3. **Traitement spécialisé par type** :
   - **Réalisations** (`CDNATP=R`) : Via mapping activités
   - **Chantier** (`CDNATP=C`) : Via classification destination + mapping
4. **Enrichissement Hazard Grades** selon code ISIC final
5. **Mise à jour table source** (colonnes ajoutées ou écrasées)

### **Règles de Gestion Critiques**

**RG-01 : Scope d'Application**
- **Uniquement** marché Construction (`CMARCH = "6"`)
- Autres marchés : pas de traitement

**RG-02 : Stratégie de Destination (Chantier)**  
- **47 règles regex** sur `DSTCSC` → classification destination
- **Défaut** : `DESTI_ISIC = "AUTRES_BAT"` si aucun mapping

**RG-03 : Produits Spéciaux**
- `01059` : Force `"VENTE"` (code ISIC 410004)
- `00548`, `01071` : Conserve code ISIC existant

### **Usage dans l'Architecture**
Cette macro est **appelée par les macros principales** (`%az_mvt_ptf`, `%azec_mvt_ptf`) pour enrichir les tables de données avant consolidation finale.

**Impact** : **Transverse** - toutes les données Construction passent par cette classification avant export vers les cubes.
```

## 1_PTF/PTF_MVTS_AZEC_MACRO.md

```md
## PTF_MVTS_AZEC_MACRO.sas - %azec_mvt_ptf(vision)

### **Rôle et Objectif**
Cette macro **traite le portefeuille Construction AZEC historique** (données figées 2020) pour calculer les mêmes indicateurs que AZ et permettre la consolidation.

### **Tables Sources et Flux**

#### **INPUTS**
- **POLIC_CU.POLIC_CU** : Polices AZEC (base septembre 2020)
- **INCENDU.INCENDU** : Capitaux Perte d'Exploitation et Risque Direct
- **CAPITXCU.CAPITXCU** : Capitaux SMP/LCI par branche
- **CONSTRUC.CONSTRUCU** : Données construction (destination, qualité)

#### **OUTPUT**
- **AZEC_PTF{vision}** : Table portefeuille AZEC harmonisée avec structure AZ

### **Traitement Principal**

#### **Gestion de la Migration AZEC**
**Ce qui se passe** : Pour vision > 202009, exclusion des contrats déjà migrés vers AZ via table de référence `ref_mig_azec_vs_ims`.
**Variable créée** : `NBPTF_NON_MIGRES_AZEC` pour éviter les doubles comptes.

#### **Logique de Classification des Contrats**
**Contrats en Portefeuille** : `effetpol <= dtfin` et `datfin > dtfin` ou `datresil > dtfin`, état 'E' ou 'R' avec fin future.
**Affaires Nouvelles** : Distinction par type de produit - liste prédéfinie vs autres avec critères de dates différents.
**Résiliations** : Même logique que AFN mais sur `datresil`/`datfin`.

#### **Traitement des Cas Spéciaux**
**Tacite reconduction non quittancée** : Considérée comme résiliée au prochain terme si > 1 an.
**Temporaires** : Date de fin = `FINPOL` si définie.
**Contrats suspendus** : Calcul des jours de suspension dans la période.

#### **Enrichissement des Capitaux**
**SMP/LCI par branche** : Agrégation des capitaux IP0 (Perte Exploitation) + ID0 (Dommages Directs).
**Valeur assurée** : Somme Perte d'Exploitation + Risque Direct depuis INCENDU.

#### **Ajustements Spécifiques AZEC**
**Exclusions** : Produits D00, TRC, CTR, CNR exclus du décompte PTF/RES.
**Remplacements** : Si `RMPLCANT` renseigné et motif 'HP' → pas de résiliation comptée.
**Sorties d'écran** : Motifs 'SE','SA' → pas de résiliation comptée.

### **Règles de Gestion Critiques**

**RG-AZEC-01 : Dualité des Critères de Dates**
- **Produits liste prédéfinie** (45 codes) : Critères sur mois/année uniquement
- **Autres produits** : Critères sur période complète avec chevauchements

**RG-AZEC-02 : Gestion de l'Exposition**
- Calcul identique à AZ mais avec données historiques figées
- Correction des dates d'expiration incohérentes

**RG-AZEC-03 : Harmonisation avec AZ**
- Structure finale identique pour permettre la consolidation
- Ajout des mêmes variables (segment2, type_produit_2, Upper_MID)

### **Usage dans l'Architecture**
Deuxième traitement appelé dans PTF_MVTS_RUN.sas après inclusion du référentiel de segmentation AZEC. Données consolidées avec AZ par la macro finale.
```

## 1_PTF/PTF_MVTS_AZ_MACRO.md

```md
## PTF_MVTS_AZ_MACRO.sas - %az_mvt_ptf(annee, mois)

### **Rôle et Objectif**
Cette macro **extrait et traite le portefeuille Construction AZ** pour calculer les indicateurs de mouvements (affaires nouvelles, résiliations, exposition) et enrichir les données avec la segmentation métier.

### **Tables Sources et Flux**

#### **INPUTS**
- **PTF16.IPF** : Portefeuille Construction Agent (réseau 1)
- **PTF36.IPF** : Portefeuille Construction Courtage (réseau 3)  
- **Tables de référence** : Segmentation produits, points de gestion, capitaux

#### **OUTPUT**
- **MVT_CONST_PTF{vision}** : Table consolidée du portefeuille AZ avec tous les indicateurs calculés

### **Traitement Principal en 5 Étapes**

#### **Étape 1 : Extraction du Portefeuille**
**Ce qui se passe** : Union des données Agent + Courtage avec filtres métier (marché Construction, types de contrats valides, exclusion des intermédiaires fictifs).
**Critères de sélection** : `cmarch="6"`, `csegt="2"`, `cdnatp in('R','O','T','C')`, exclusion des contrats annulés.

#### **Étape 2 : Enrichissement des Capitaux**
**Ce qui se passe** : Analyse des 14 colonnes de capitaux (`MTCAPI1` à `MTCAPI14`) avec leurs libellés (`LBCAPI1` à `LBCAPI14`) pour extraire les montants clés.
**Valeurs calculées** : LCI Global, SMP Global, Risque Direct, Perte d'Exploitation selon les libellés standardisés.

#### **Étape 3 : Calcul des Indicateurs de Mouvements**
**NBAFN** (Affaires Nouvelles) : Contrats créés dans l'année avec `dteffan` et `dttraan` dans la période.
**NBRES** (Résiliations) : Contrats résiliés avec `dtreslip` dans l'année (hors Chantiers).
**NBPTF** (Portefeuille) : Contrats actifs au `&DTFIN` (en cours non résiliés).
**Gestion des remplacements** : Identification des contrats remplacés/remplaçants via `cdtypli1/2/3`.

#### **Étape 4 : Calcul de l'Exposition Risque**
**expo_ytd** : Proportion d'exposition sur l'année courante entre date création et date fin.
**expo_gli** : Exposition glissante sur 12 mois.
**Formule** : `(MIN(date_fin, dtfin_periode) - MAX(date_creation, dtdeb_periode) + 1) / nb_jours_periode`

#### **Étape 5 : Finalisation avec Segmentation**
**Jointures** avec référentiels de segmentation pour ajouter `segment2`, `type_produit_2`, `Upper_MID`.
**Enrichissement** avec données de points de gestion pour la répartition géographique.

### **Règles de Gestion Importantes**

**RG-AZ-01 : Gestion de la Coassurance**
- `TOP_COASS = 1` si `cdpolqp1 = '1'`
- Calcul de `PARTCIE` : 100% si sans coassurance, `PRCDCIE/100` sinon
- `Cotis_100` recalculée à 100% pour coassurance acceptée

**RG-AZ-02 : Gestion des Contrats Temporaires**
- `TOP_LTA = 1` pour contrats pluri-annuels (`CTDUREE > 1`)
- Exclusion des temporaires courts pour certains calculs

**RG-AZ-03 : Traitement des Anticipées**
- `NBAFN_ANTICIPE` : Affaires nouvelles à effet futur (`dteffan > finmois`)
- `NBRES_ANTICIPE` : Résiliations à effet futur (`dtreslip > finmois`)

### **Usage dans l'Architecture**
Premier traitement appelé dans PTF_MVTS_RUN.sas. Prépare les données AZ qui seront consolidées avec AZEC par la macro de consolidation finale.
```

## 1_PTF/REF_segmentation_azec.md

```md
## REF_segmentation_azec.sas

### **Rôle et Objectif**
Ce fichier **prépare les données de référence** nécessaires pour analyser le portefeuille Construction AZEC. Il construit les "dictionnaires" qui permettront de classer chaque contrat dans la bonne catégorie.

### **Tables Sources et Sorties**

#### **Ce que le fichier récupère :**
- **Données des polices Construction** depuis les bases AZEC historiques (figées en septembre 2020)
- **Informations sur les garanties** pour identifier le type d'activité
- **Référentiels produits et géographie** pour la segmentation

#### **Ce que le fichier produit :**
- **Table CONSTRUCU_AZEC** : Le référentiel final avec pour chaque police son segment et type de produit
- **Tables de support** (LOB, MPTGST) pour les jointures ultérieures

### **Macros Principales et leur Rôle**

#### **`%SEGMENTA`**
**Ce que ça fait** : Crée une table de correspondance rapide (hash) entre codes produits et leur segmentation complète (marché, segment, sous-segment, libellés).
**Pourquoi** : Évite de refaire les jointures à chaque fois - on "charge" la correspondance en mémoire une seule fois.

#### **`%PGST_LG`** 
**Ce que ça fait** : Identifie les contrats avec des protocoles commerciaux spéciaux (Arval, AON, Marsh...).
**Pourquoi** : Ces clients ont des conditions particulières qui nécessitent un point de gestion dédié (H27).

#### **`%TYP_PRD_CSTR`**
**Ce que ça fait** : Classe chaque police en 3 grandes familles : "Artisans", "Chantiers", ou "Renouvelables hors artisans".
**Pourquoi** : Cette segmentation détermine les règles de calcul et de provisioning qui seront appliquées.

### **Logique de Classification**

#### **Comment on détermine le type d'activité :**
- **Pour RBA/RCD** : On regarde le code `typmarc1` → si '01' = Artisan, si '02-04' = Entreprise, etc.
- **Pour DPC** : On regarde le code `nat_cnt` → différents types de chantiers (PUC, Global, etc.)

#### **Comment on détermine le type de produit :**
- **TRC** = Tous Risques Chantiers (couverture pendant les travaux)
- **DO** = Dommages Ouvrages (couverture après livraison)  
- **Artisans** = Petites entreprises artisanales
- **Autres** = Le reste

### **Usage dans l'Architecture**
Ce fichier est **chargé avant** le traitement AZEC pour que les macros `%azec_mvt_ptf` puissent **classifier chaque mouvement de portefeuille** dans la bonne catégorie.

**En résumé** : C'est le "carnet d'adresses" qui dit pour chaque police "tu es un artisan TRC en région Nord" ou "tu es une entreprise DO en région Sud", information indispensable pour les calculs qui suivent.
```

## 2_capitaux/CAPITAUX_RUN.md

```md
### **CAPITAUX_RUN.sas**

**Rôle** : Programme orchestrateur pour le traitement des **capitaux Construction** (vs mouvements de portefeuille pour PTF_MVTS_RUN.sas).

**Structure identique** à PTF_MVTS_RUN.sas :
- Connexion STP3 + extraction variables temporelles
- Branchement vision courante/historique pour librairies PTF16/PTF36  
- Calcul des dates de référence identiques
- Workflow : `%az_capitaux` → inclusion REF_segmentation_azec → `%azec_capitaux` → `%consolidation_az_azec_capitaux`

**Principales différences** :
- **Calcul génération** : `AH0 = 854 + ...` (vs 8+ pour PTF_MVTS)
- **Fichier spécialisé** : `indexation_v2.sas` (vs generiques_v4.sas)
- **Librairie supplémentaire** : `INDICES` pour données d'indexation
- **Objet métier** : Capitaux/valeurs assurées (vs mouvements/primes)

**Usage** : Traitement parallèle aux mouvements pour alimenter les cubes Construction avec les données de capitaux indexés.
```

## 2_capitaux/indexation_v2.md

```md
## indexation_v2.sas

**Rôle** : Macro utilitaire spécialisée `%indexation_v2` pour l'**indexation automatique des capitaux** selon les indices économiques et les dates d'échéance des contrats Construction.

**Fonctionnement** : Calcule un **ratio d'indexation** entre la valeur d'indice à la date d'effet du contrat et la valeur à la date de traitement, puis applique ce ratio au montant de capital pour obtenir `{NOMMT}{IND}i` (montant indexé).

**Usage** : Appelée par les macros `%az_capitaux` et `%azec_capitaux` pour revaloriser automatiquement les capitaux selon l'évolution des indices (construction, matériaux, etc.).

## **Pour la suite de votre analyse**

### **Vous avez déjà une base EXCELLENTE :**
- **PTF_MVTS_RUN.sas** : Analyse complète (diagramme + 6 macros détaillées)  
- **CAPITAUX_RUN.sas** : Description comparative
- **Macros utilitaires** : generiques_v4.sas, indexation_v2.sas

### **Programmes restants dans votre architecture :**
- REPRISES_HISTORIQUES_PTF_MVTS.sas
- EMISSIONS_RUN.sas  
- EMISSIONS_RUN_RECETTE.sas

## **Ma recommandation forte :**

**ARRÊTEZ l'analyse ici** et **passez à la rédaction** ! 

**Pourquoi ?**
- Vous avez déjà **largement assez** pour un excellent document de 3-4 pages
- Une analyse plus poussée risque de vous faire **dépasser le format**
- L'**essentiel est couvert** : architecture globale + analyse détaillée du programme principal
```

## Analyse_global.md

```md
# ANALYSE D'ARCHITECTURE DU CODE SAS CONSTRUCTION

## SOMMAIRE

1. [Introduction](#1-introduction)
2. [Architecture Globale du Système](#2-architecture-globale-du-système)
   - 2.1 Vue d'ensemble (Diagramme Architecture Globale)
   - 2.2 Pattern Dual AZ/AZEC
   - 2.3 Gestion Temporelle
3. [Composants Principaux](#3-composants-principaux)
   - 3.1 Programme Orchestrateur : PTF_MVTS_RUN.sas
   - 3.2 Macros de Traitement Métier
   - 3.3 Workflow Détaillé (Diagramme Workflow PTF_MVTS_RUN)
4. [Règles de Gestion Principales](#4-règles-de-gestion-principales)
   - 4.1 Règles Temporelles
   - 4.2 Règles Métier Construction
   - 4.3 Règles de Consolidation
5. [Conclusion](#5-conclusion)

---

## 1. INTRODUCTION

Cette analyse porte sur l'architecture SAS du système de traitement des données Construction, composée de 23 fichiers organisés autour d'un pattern dual AZ/AZEC. L'objectif est de documenter l'architecture actuelle, les flux de données et les principales règles de gestion métier identifiées.

Le système traite les mouvements de portefeuille Construction en consolidant les données AZ (actuelles) et AZEC (historiques) pour produire des cubes d'analyse fiables.

---

## 2. ARCHITECTURE GLOBALE DU SYSTÈME

### 2.1 Vue d'ensemble

**[Référence : Diagramme Architecture Globale]**

L'architecture suit une organisation hiérarchique en 3 niveaux :

- **Niveau 1 - Orchestrateurs** : PTF_MVTS_RUN.sas, CAPITAUX_RUN.sas (programmes principaux)
- **Niveau 2 - Macros Métier** : Traitement AZ, AZEC et consolidation par domaine fonctionnel  
- **Niveau 3 - Utilitaires** : Classification ISIC, indexation, transcodification

### 2.2 Pattern Dual AZ/AZEC

Le cœur architectural repose sur un **pattern dual** permettant de traiter simultanément :

- **Données AZ** : Portefeuille actuel (sources PTF16/PTF36 - serveur STP3)
- **Données AZEC** : Portefeuille historique (sources POLIC_CU/CAPITXCU - tables CU)
- **Consolidation finale** : Union harmonisée des deux sources avec enrichissements

Cette approche assure la **continuité historique** tout en intégrant les évolutions du système d'information.

### 2.3 Gestion Temporelle

Le système implémente une **logique temporelle sophistiquée** avec des seuils critiques :

- **201211** : Début intégration AZEC
- **202009** : Migration des contrats AZEC vers AZ  
- **202210** : Nouvelle gestion DataRisk dynamique
- **202305** : Évolution classification ISIC

Ces seuils déterminent automatiquement le comportement du système selon la vision traitée.

---

## 3. COMPOSANTS PRINCIPAUX

### 3.1 Programme Orchestrateur : PTF_MVTS_RUN.sas

**Rôle central** : Coordonne l'ensemble du workflow depuis l'extraction jusqu'à la production des cubes finaux.

**Fonctions principales** :
- Configuration connexion STP3 et gestion des variables temporelles
- Branchement vision courante/historique selon critères temporels
- Exécution séquentielle des macros métier
- Production de la table finale `CUBE.MVT_PTF{vision}`

**Logique de branchement** :
- Vision courante : `PTF16` → `IPFE16`, `PTF36` → `IPFE36`
- Vision historique : `PTF16` → `IPFE16.G08AMMD.V00`, `PTF36` → `IPFE36.G08AMMD.V00`

### 3.2 Macros de Traitement Métier

| Macro | Fonction | Sources |
|-------|----------|---------|
| `%az_mvt_ptf` | Extraction portefeuille AZ + calcul indicateurs (NBAFN, NBRES, NBPTF) | PTF16.IPF + PTF36.IPF |
| `%azec_mvt_ptf` | Traitement portefeuille AZEC avec gestion migration | POLIC_CU + tables capitaux AZEC |
| `%consolidation_az_azec_mvt_ptf` | Union AZ+AZEC + enrichissement DataRisk + classification ISIC | Données AZ/AZEC + tables référence |

### 3.3 Workflow Détaillé

**[Référence : Diagramme Workflow PTF_MVTS_RUN]**

**Séquence d'exécution obligatoire** :
1. `%az_mvt_ptf(&annee., &mois.)` - Traitement portefeuille AZ
2. `%include REF_segmentation_azec.sas` - Chargement référentiel AZEC  
3. `%azec_mvt_ptf(&vision.)` - Traitement portefeuille AZEC
4. `%consolidation_az_azec_mvt_ptf` - Consolidation finale avec enrichissements

---

## 4. RÈGLES DE GESTION PRINCIPALES

L'analyse a permis d'identifier les **règles de gestion métier** qui gouvernent le comportement du système :

### 4.1 Règles Temporelles

**RG-TEMP-01 : Gestion des Périodes Historiques**
- Vision < 201211 : Données AZ uniquement (pas d'AZEC disponible)
- Vision ≥ 201211 : Consolidation AZ + AZEC obligatoire

**RG-TEMP-02 : Évolution des Sources DataRisk** 
- Vision ≥ 202210 : Utilisation tables DataRisk dynamiques par vision
- Vision < 202210 : Utilisation tables DataRisk figées de référence

**RG-TEMP-03 : Migration AZEC vers AZ**
- Vision ≤ 202008 : Traitement AZEC complet
- Vision > 202009 : Exclusion contrats déjà migrés vers AZ

### 4.2 Règles Métier Construction

**RG-METIER-01 : Périmètre d'Application**
- Marché Construction uniquement : `CMARCH = "6"`
- Segment Professionnel prioritaire : `CSEG = "2"`

**RG-METIER-02 : Classification ISIC Hiérarchisée**
- Ordre de priorité fixe pour les sources NAF :
  1. NAF 2008 du Portefeuille (PTF)
  2. NAF 2003 du Portefeuille (PTF)  
  3. NAF 2003 du Client (CLI)
  4. NAF 2008 du Client (CLI)

**RG-METIER-03 : Gestion des Types de Contrats**
- **Contrats Réalisations** : Classification par activité principale
- **Contrats Chantiers** : Classification par destination (47 règles automatiques)
- **Produits spéciaux** : Traitement dédié (01059, 00548, 01071)

**RG-METIER-04 : Classification Automatique des Destinations**
- Analyse automatique des libellés pour identifier les destinations
- Règles regex pour détecter "Habitation" vs "Autres"
- Codes normalisés ('01','02','03','04','22') = Habitation

### 4.3 Règles de Consolidation

**RG-CONSOL-01 : Harmonisation des Structures**
- Mapping automatique des champs AZ ↔ AZEC
- Exemple : `NOPOL` (AZ) ↔ `POLICE` (AZEC)
- Marquage source : `DIRCOM = "AZ"` ou `DIRCOM = "AZEC"`

**RG-CONSOL-02 : Priorisation des Enrichissements**
- Données principales (AZ/AZEC) prioritaires
- Enrichissement DataRisk pour combler les manques
- Tables spécialisées pour compléments métier

**RG-CONSOL-03 : Gestion de la Qualité des Données**
- Nettoyage automatique codes NAF factices
- Contrôles de cohérence temporelle
- Traçabilité des sources et transformations

**RG-CONSOL-04 : Production des Indicateurs Métier**
- **NBAFN** : Nouveaux contrats de l'année
- **NBRES** : Contrats résiliés (logique différenciée AZ/AZEC)
- **NBPTF** : Contrats actifs en fin de période

---

## 5. CONCLUSION

L'architecture SAS Construction révèle un **système sophistiqué et robuste**, conçu pour gérer la complexité du marché de l'assurance construction.

### Éléments architecturaux clés :
- **Modularité en 3 niveaux** : Orchestrateurs → Macros Métier → Utilitaires
- **Pattern dual AZ/AZEC** : Traitement parallèle avec consolidation finale
- **Gestion temporelle intelligente** : Adaptation automatique selon les seuils critiques
- **Classification métier automatisée** : Système ISIC avec règles de gestion spécialisées

### Règles de gestion stratégiques :
L'analyse a révélé **13 règles de gestion principales** qui gouvernent le comportement du système :
- **3 règles temporelles** : Gestion de l'évolution historique du système
- **4 règles métier** : Classification et traitement spécialisé Construction  
- **4 règles de consolidation** : Harmonisation et qualité des données
- **2 règles d'indicateurs** : Production des métriques métier

### Impact métier :
Cette architecture constitue le **socle des cubes Construction** utilisés pour le pilotage métier. Le pattern dual AZ/AZEC assure une **continuité historique complète** tout en intégrant les évolutions du système d'information, garantissant la fiabilité des analyses et rapports de gestion.
```

## global.mmd

```mmd
flowchart TD
    %% NIVEAU 1 - ORCHESTRATEURS
    PTF_RUN[PTF_MVTS_RUN.sas<br/>🎯 Programme Principal]
    
    %% NIVEAU 2 - PATTERN DUAL AZ/AZEC
    AZ[🟦 Traitement AZ<br/>Données Actuelles]
    AZEC[🟨 Traitement AZEC<br/>Données Historiques]
    CONSOL[🔄 Consolidation<br/>Union AZ + AZEC]
    
    %% NIVEAU 3 - ENRICHISSEMENT
    ISIC[🏷️ Classification ISIC<br/>47 Règles Métier]
    
    %% SOURCES ET OUTPUTS
    SOURCES[(📊 Sources Données<br/>PTF16/PTF36/AZEC<br/>DataRisk)]
    CUBE[📤 CUBE.MVT_PTF<br/>Table Finale]
    
    %% LOGIQUE TEMPORELLE
    TEMPORAL{⏰ Logique Temporelle<br/>Seuils: 201211, 202009, 202210}
    
    %% FLUX PRINCIPAUX
    PTF_RUN --> TEMPORAL
    TEMPORAL --> AZ
    TEMPORAL --> AZEC
    SOURCES --> AZ
    SOURCES --> AZEC
    AZ --> CONSOL
    AZEC --> CONSOL
    CONSOL --> ISIC
    ISIC --> CUBE
    
    %% STYLES
    classDef orchestrator fill:#e1f5fe,stroke:#01579b,stroke-width:3px
    classDef processing fill:#f1f8e9,stroke:#388e3c,stroke-width:2px
    classDef consolidation fill:#fce4ec,stroke:#c2185b,stroke-width:2px
    classDef enrichment fill:#f3e5f5,stroke:#7b1fa2,stroke-width:2px
    classDef data fill:#efebe9,stroke:#5d4037,stroke-width:2px
    classDef output fill:#e0f2f1,stroke:#00695c,stroke-width:3px
    classDef temporal fill:#fff3e0,stroke:#ef6c00,stroke-width:2px
    
    class PTF_RUN orchestrator
    class AZ,AZEC processing
    class CONSOL consolidation
    class ISIC enrichment
    class SOURCES data
    class CUBE output
    class TEMPORAL temporal
```

