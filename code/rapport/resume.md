# RÉSUMÉ - Rapport de Stage
## Migration du Datamart Construction de SAS vers PySpark

**Auteur** : [Votre nom]  
**Entreprise** : Allianz France - Direction P4D - Pôle Advanced Data & Climate  
**Période** : Novembre 2024 - Avril 2025 (6 mois)  
**Encadrant** : [Nom du maître de stage]

---

## Résumé

Dans le cadre du projet "SAS EXIST", Allianz France modernise son infrastructure de données en migrant ses datamarts de SAS vers des technologies cloud modernes. Ce stage de six mois visait à créer le premier prototype de cette migration en travaillant sur le datamart Construction (marché 6), le plus accessible des cinq marchés P&C (Property & Casualty).

Le projet s'est articulé autour de cinq phases méthodologiques. La première phase a consisté en une analyse exhaustive du code SAS existant (19 fichiers, environ 15 000 lignes) et la création d'une documentation technique complète, en l'absence de documentation initiale. La deuxième phase a porté sur la conception d'une architecture cible moderne : l'architecture médaillon (Bronze/Silver/Gold), validée comme standard de l'industrie. La troisième phase a permis le recensement systématique des règles de gestion et sources de données dans des fichiers Excel de traçabilité. La quatrième phase a consisté en l'implémentation de trois pipelines Python (PTF Mouvements, Capitaux, Émissions) avec PySpark, en suivant une approche itérative rigoureuse. Enfin, la cinquième phase a validé le fonctionnement des pipelines avec des tests unitaires et des comparaisons KPI avec SAS sur 10 visions différentes.

Les résultats obtenus démontrent la faisabilité technique de la migration. Les trois pipelines (PTF Mouvements pour le portefeuille, Capitaux pour l'indexation, et Émissions pour les primes) sont fonctionnels et produisent des résultats cohérents. Les validations montrent une correspondance exacte avec les comptages SAS, confirmant la parité fonctionnelle entre les deux systèmes.

Ce travail établit les fondations méthodologiques et techniques pour la migration des quatre autres marchés, avec des gains attendus en réduction de coûts de licences SAS, amélioration des performances grâce au traitement distribué, et scalabilité cloud. Le prototype démontre qu'une migration rigoureuse vers PySpark est viable tout en garantissant la parité fonctionnelle avec l'existant. La documentation détaillée et la méthodologie éprouvée serviront de référence pour les prochaines migrations.

**Mots-clés** : Migration SAS, PySpark, Architecture médaillon, Assurance Construction, Datamart, Azure, Databricks, Data Engineering

---

**Nombre de mots** : 298 mots

---

## Notes de Conformité

**Structure du résumé** :
- ✅ Contexte (projet SAS EXIST, objectif migration)
- ✅ Méthodologie (5 phases détaillées)
- ✅ Résultats (3 pipelines fonctionnels, validation 10 visions, parité SAS)
- ✅ Perspectives (extension 4 marchés, gains économiques/performances)

**Format** :
- ✅ ~300 mots en français uniquement
- ✅ Mots-clés (8 termes techniques pertinents)
- ✅ Pas de citations, pas de références
- ✅ Auto-suffisant (compréhensible sans lire le rapport complet)
- ✅ Ton professionnel, moins de détails techniques quantitatifs

