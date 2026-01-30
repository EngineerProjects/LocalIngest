ANALYSE COMPLÈTE PROJET PTF_MVT - MIGRATION SAS → PYSPARK

█ CONTEXTE GÉNÉRAL
Migration d'un système de reporting d'assurance construction (Portefeuille Mouvements)
- Source : SAS Enterprise Guide (production actuelle)
- Cible : PySpark sur Azure Databricks
- Périmètre : 3 pipelines interconnectés (AZ, AZEC, CONSOLIDATION)
- Vision exemple : 202509 (Septembre 2025)

█ ARCHITECTURE PROJET

📂 PYTHON : /home/amiche/Projects/LocalIngest/code/new_python/

1️⃣ PIPELINES PRINCIPAUX (src/)
├── ptf_mvt_run.py
├── read.py
├── processors/
│   ├── base_processor.py
│   ├── ptf_mvt_processors/
│   │   ├── az_processor.py (~900 lignes)
│   │   └── Pipeline IPF AZ (Assurance Construction - données IMS)
│   │   ├── azec_processor.py (~1,400 lignes)
│   │   └── Pipeline AZEC (Assurance Construction - données legacy)
│   └── consolidation_processor.py (~1,100 lignes)
    └── Consolidation AZ + AZEC + harmonisation schémas

2️⃣ CONFIGURATIONS
├── config/reading_config.json (file groups, schemas, dateFormats)
├── config/schemas.py (StructType pour tous les CSV)
├── constants.py (constantes utilisées dans les transformations)
├── variables.py (variables utilisées dans les transformations)
├── config/transformations/
│   ├── az_transformations.json
│   ├── business_rules.json
│   ├── azec_transformations.json
│   └── consolidation_mappings.json

3️⃣ TRANSFORMATIONS COMMUNES (utils/transformations/)
├── enrichment/
│   ├── segmentation_enrichment.py (LOB, CONSTRCU)
│   ├── risk_enrichment.py (destinat, capital)
│   ├── destinat_enrichment.py (destinat)
│   └── client_enrichment.py (naf)
├── operations/
│   └── business_logic.py (business rules - mouvements, expositions, primes)
└── base/
    ├── isic_codification.py
    ├── column_operations.py
    └── generic_transforms.py

4️⃣ MAIN
├── main.py

📂 SAS : /home/amiche/Projects/LocalIngest/code/sas/

├── PTF_MVTS_AZ_MACRO.sas (~400 lignes) → Baseline AZ
├── PTF_MVTS_AZEC_MACRO.sas (~490 lignes) → Baseline AZEC
├── CONSOLID_AZ_AZEC.sas (~300 lignes) → Baseline CONSOLIDATION
├── REF_segmentation_azec.sas (~345 lignes) → Référentiel segmentation
└── CODIFICATION_ISIC_CONSTRUCTION.sas

█ FLUX DE DONNÉES

INPUT (Bronze Layer - CSV):
├── AZ: ipf16.csv, ipf32.csv, ipfm99.csv (~100k lignes)
├── AZEC: polic_cu.csv, capitxcu.csv, incendcu.csv (~2M lignes)
└── REF: lob.csv, constrcu.csv, segmentation tables

PROCESSING:
1. AZ Pipeline → az_ptf_YYYYMM (Silver)
2. AZEC Pipeline → azec_ptf_YYYYMM (Silver)
3. CONSOLIDATION → ptf_consolide_YYYYMM (Gold)

OUTPUT:
└── Delta tables avec schéma harmonisé unifié

█ OBJECTIFS DE L'ANALYSE

🎯 PRIORITÉ 1 - CONFORMITÉ MÉTIER
Pour CHAQUE pipeline (AZ, AZEC, CONSOLIDATION) :
1. Mapper étapes Python ↔ SAS ligne par ligne
2. Identifier écarts de logique métier (filtres, calculs, transformations)
3. Valider row counts (input → output à chaque étape)
4. Vérifier calculs clés :
   - Mouvements : NBPTF, NBAFN, NBRES
   - Expositions : EXPO_YTD, EXPO_GLI
   - Primes : PRIMETO, PRIMES_AFN, PRIMES_RES, PRIMES_PTF

🎯 PRIORITÉ 2 - COHÉRENCE INTER-PIPELINES
1. Harmonisation schémas (colonnes communes AZ/AZEC)
2. Mapping colonnes lors de la consolidation
3. Gestion des duplicates et des clés (POLICE, PRODUIT, NOPOL, CDPROD)
4. Résolution des conflits de nommage

🎯 PRIORITÉ 3 - DONNÉES & CONFIGURATIONS
1. Formats dates (dateFormat dans reading_config.json pour ~30 file groups)
2. Schémas PySpark (SCHEMA_REGISTRY vs CSV réels)
3. Gestion NULL SAS (".", "", " ") vs Python (None)
4. Encodage (LATIN9 vs UTF-8)

🎯 PRIORITÉ 4 - PERFORMANCE & QUALITÉ CODE
1. Utilisation broadcast() pour références
2. Stratégie caching/persisting
3. Order of operations (filter → join → select)
4. Code duplication entre AZ/AZEC
5. Patterns communs à factoriser

█ PLAN D'ANALYSE SUGGÉRÉ

PHASE 1 - VUE D'ENSEMBLE (1-2h)
✓ Lire README et documentation existante
✓ Parcourir structure de chaque pipeline (read → transform → write)
✓ Identifier transformations communes utilisées
✓ Créer diagramme architecture global

PHASE 2 - AUDIT DÉTAILLÉ PAR PIPELINE (3-6h)
Pour AZ, AZEC, CONSOLIDATION :
✓ Comparer étapes Python vs SAS
✓ Vérifier file groups utilisés
✓ Valider schémas et configs
✓ Tester sur vision 202509

PHASE 3 - PATTERNS COMMUNS (2-3h)
✓ Segmentation LOB (utilisée par AZ et AZEC)
✓ Enrichissement capitaux
✓ Calcul mouvements
✓ Indexation primes

PHASE 4 - CONSOLIDATION (2-3h)
✓ Harmonisation schémas AZ/AZEC
✓ Mapping colonnes (rename/computed)
✓ Gestion conflits
✓ Union finale

PHASE 5 - Duplication des transformations
✓ Duplication des transformations
✓ Detecter les transformations ou fonction ou operations qui font exactement les mêmes chose et comments les gérées, ie les supprimés, et réorganiser comme il faut pour les mettres dans le bon enplacement. surtout les fichiers dans utils/transformations

█ LIVRABLES ATTENDUS

📊 1. RAPPORT ARCHITECTURE GLOBALE
- Diagramme flux de données (Mermaid)
- Tableau comparatif 3 pipelines
- Matrice dépendances (file groups × processors)
- Statistiques code (LOC, complexité, coverage)

📋 2. ANALYSE CONFORMITÉ PAR PIPELINE
Pour chaque pipeline (AZ/AZEC/CONSOLIDATION) :
- Tableau étapes Python ↔ SAS
- Score conformité (0-100%)
- Liste écarts priorisés
- Recommandations corrections

🔍 3. AUDIT CONFIGURATIONS
- Validation reading_config.json (29 file groups)
- Audit dateFormats (résultats script date_format_audit.py)
- Vérification schemas.py (SCHEMA_REGISTRY)
- Mapping transformations JSON

🧪 4. PLAN VALIDATION E2E
- Detection des duplications dans les transformations pour avoir un code propre et lisible et logique.
- Critères succès globaux (row counts, sums, distributions)
- Tests inter-pipelines (cohérence AZ/AZEC dans consolidation)
- Commandes exécution complète
- Comparaison SAS vs Python outputs

📝 5. ROADMAP AMÉLIORATION
- Quick wins (corrections rapides)
- Refactoring moyen terme (patterns communs)
- Optimisations performance
- Documentation manquante

█ QUESTIONS À RÉSOUDRE

❓ ARCHITECTURE
- Pourquoi 2 pipelines séparés (AZ vs AZEC) au lieu d'un unifié ?
- Quelle est la logique de dispatch (IMS vs legacy) ?
- Y a-t-il des overlaps de données entre AZ et AZEC ?

❓ TRANSFORMATIONS
- Quelles transformations sont identiques entre AZ/AZEC ?
- Où sont les différences de logique métier ?
- Peut-on factoriser du code commun ?

❓ DONNÉES
- Tous les file groups sont-ils utilisés actuellement ?
- Quels sont les volumes réels (min/avg/max par vision) ?
- Y a-t-il des dépendances temporelles (visions antérieures) ?

❓ QUALITÉ
- Existe-t-il des tests unitaires ?
- Y a-t-il une validation automatique vs SAS ?
- Comment gérer les régressions ?
- Existe-t-il des duplications dans les transformations ?
- Comment gérer les transformations qui font exactement les mêmes chose et comments les gérées, ie les supprimés, et réorganiser comme il faut pour les mettres dans le bon enplacement. surtout les fichiers dans utils/transformations

█ CONTRAINTES & RÈGLES

✅ DO:
- TOUJOURS citer sources (fichier + ligne) pour SAS ET Python
- Valider CHAQUE affirmation avec le code source
- Proposer corrections minimales (SAS-faithful)
- Documenter les écarts non-résolus avec justification

❌ DON'T:
- Ne PAS supposer sans vérifier le code
- Ne PAS optimiser prématurément (conformité d'abord)
- Ne PAS ignorer les warnings/edge cases
- Ne PAS modifier les configs sans comprendre l'impact

Commence par créer une vue d'ensemble de l'architecture, puis analyse AZ, AZEC et CONSOLIDATION dans cet ordre.