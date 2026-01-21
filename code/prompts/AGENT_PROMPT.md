### Prompt

Tu es un **expert SAS**, **PySpark**, et **architecture Médaillon (bronze/silver/gold)**.
Je travaille sur une **migration SAS → Python (PySpark)** et mon objectif est une **parité fonctionnelle exacte** : mon code PySpark doit reproduire **strictement** les mêmes résultats que le SAS (mêmes règles, mêmes priorités, mêmes filtres, mêmes champs produits, mêmes valeurs par défaut, mêmes comportements sur NULL/vides, mêmes dedup/tri, etc.).

Je vais me concentrer sur la **partie AZEC**.

#### 1) Contexte

* Pipeline PySpark avec architecture Médaillon : lecture bronze → transformations → silver → consolidation/enrichissement → gold.
* Je veux une **comparaison ligne à ligne / logique par logique** entre SAS et PySpark.
* Je veux que tu identifies tout ce qui peut créer un écart :

  * différences NULL vs vide `""`
  * différences `WHERE` (SAS) vs `filter` (Spark)
  * équivalences `merge/update` SAS vs `join` Spark
  * `nodupkey` / tri / priorité / hash join SAS
  * formats de dates et conversions implicites SAS
  * `substr`, `trim`, `upcase`, `compress`, gestion espaces
  * regex / PRXMATCH vs rlike
  * priorités de `if/else` SAS vs `when` Spark
  * ordre d’évaluation (SAS “last write wins”)
  * casting / types / arrondis / doubles / int
  * union corr / unionByName
  * règles métiers manquantes ou ajoutées par erreur
  * colonnes manquantes, colonnes en trop, renommages incohérents

#### 2) Fichiers Python à analyser

Lis attentivement ces fichiers PySpark (tous) et comprends le flux complet :

* `code/new_python/config/transformations/azec_transformations.json`
* `code/new_python/config/constants.py`
* `code/new_python/config/config.yml`
* `code/new_python/config/reading_config.json`
* `code/new_python/config/schemas.py`
* `code/new_python/config/variables.py`
* `code/new_python/src/processors/ptf_mvt_processors/azec_processor.py`
* `code/new_python/src/ptf_mvt_run.py`
* `code/new_python/src/reader.py`
* `code/new_python/utils/helpers.py`
* `code/new_python/utils/transformations/operations/business_logic.py`
* `code/new_python/utils/transformations/enrichment/segmentation_enrichment.py`
* `code/new_python/utils/transformations/base/isic_codification.py`
* `code/new_python/utils/transformations/base/generic_transforms.py`
* `code/new_python/utils/transformations/base/column_operations.py`

#### 3) Fichiers SAS de référence

Compare avec ces fichiers SAS principaux (et tout ce qu’ils appellent / macros utilisées si nécessaire) :

* `code/sas/REF_segmentation_azec.sas`
* `code/sas/PTF_MVTS_AZEC_MACRO.sas`
* `code/sas/CODIFICATION_ISIC_CONSTRUCTION.sas`

#### 4) Ta mission

1. **Reconstruis mentalement le pipeline SAS** : entrées, étapes, outputs, règles (ordre exact).
2. **Reconstruis mentalement le pipeline PySpark** : lecture, transformations, enrichissements, sorties.
3. Fais une **comparaison exhaustive** et conclus si mon PySpark est **strictement conforme** au SAS.

#### 5) Format du rapport attendu (obligatoire)

Je veux un rapport structuré en sections :

**A. Cartographie globale**

* Tableau/plan : Étape SAS → Étape PySpark correspondante (ou “manquante”)
* Entrées/sorties attendues à chaque étape (datasets, clés, cardinalités, colonnes)

**B. Comparaison par blocs métier**
Pour chaque bloc (ex : mouvements AZEC, exposures, segmentation, ISIC codification, filtres métier, etc.) :

* Règle SAS exacte (décrite clairement)
* Implémentation PySpark correspondante
* Statut : ✅ identique / ⚠️ risque d’écart / ❌ non conforme
* Différences trouvées et leur impact (sur quelles lignes / quels cas)

**C. Liste des écarts**

* Écarts bloquants (résultat différent certain)
* Écarts probables (NULL/trim/join order)
* Écarts mineurs (style/robustesse mais pas résultat)

**D. Recommandations concrètes**
Pour chaque correction :

* Pourquoi c’est nécessaire (référence SAS)
* Patch recommandé (modifs précises : où, quoi)
* Attention aux points Spark (ex: NULL, trim, types)

**E. Checklist de validation**

* Propose une stratégie de tests de parité (samples + cas extrêmes)
* Idées de “assertions” : counts, distinct keys, agrégats, distribution, top N diffs, diff par clé
* Recommandations pour logs (colonnes à tracer)

#### 6) Contraintes importantes

* **Ne te contente pas de généralités** : je veux des comparaisons concrètes.
* Si une partie manque (ex: un fichier appelé mais non fourni), indique clairement l’hypothèse et ce qu’il manque pour conclure.
* Quand tu suspectes un écart, propose une **correction précise** (ex : ajouter `trim()`, forcer `upper()`, remplacer `dropDuplicates` + orderBy par fenêtre, etc.).
* Priorité absolue : **parité SAS** avant “best practices Spark”.
* Prendre en compte pour pyspark s'il serait mieux d'avoir les filtres en python  directement ou le garder en json