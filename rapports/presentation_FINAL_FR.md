# Migration SAS → PySpark
## Datamart Construction - Présentation de Fin de Stage

**[Votre Nom] - Janvier 2025**

---

# 📑 STRUCTURE : 12 SLIDES ESSENTIELLES

---

## Slide 1 : Titre

**Titre** : Migration SAS → PySpark  
**Sous-titre** : Datamart Construction (Marché 6)

**Présenté par** : [Votre nom]  
**Stage** : Novembre 2024 - Janvier 2025 (2 mois)  
**Date de présentation** : [Date]

**VISUEL** : Logo Allianz + SAS → Python (flèche)

---

## Slide 2 : Le Projet en 1 Slide

**Titre** : Contexte et Objectif

**2 colonnes visuelles** :

**❌ AVANT (SAS)** :
- Pipeline en production
- Coûts de licence élevés  
- Technologies propriétaires
- Expertise rare

**✅ OBJECTIF** :
- Migrer vers PySpark
- Réduire les coûts
- Stack moderne (open source)
- **Garantir parité fonctionnelle**

**Périmètre** : 3 pipelines, ~1.5M contrats/mois, 45 datasets

**VISUEL** : Schéma avant/après

---

## Slide 3 : Ma Démarche en 4 Étapes

**Titre** : Comment j'ai travaillé

**TIMELINE horizontale** :

```
1. COMPRENDRE           2. RECENSER            3. CONCEVOIR           4. DÉVELOPPER
   (2-3 semaines)          (1 semaine)            (1 semaine)            (4-5 semaines)

   📖 Analyser SAS         📊 2 fichiers Excel    🏗️ Architecture        💻 3 pipelines
   📝 Documenter           • Règles métier        médaillon             Python
                           • Datasets
```

**Message clé** : "Analyser et recenser **avant** de coder"

**VISUEL** : 4 boîtes avec flèches + icônes

---

## Slide 4 : Mes Livrables d'Analyse

**Titre** : Documentation et Recensement

**3 livrables majeurs** :

**1. 📚 Documentation SAS complète**
- 19 fichiers analysés (~15k lignes)
- 2 versions : synthétique + technique
- Schémas de flux créés

**2. 📊 Excel Règles de Gestion**
- ~150 règles identifiées
- Mapping SAS → Python
- Checklist implémentation

**3. 📂 Excel Tables et Sources**
- 45 datasets recensés
- Disponibilité validée
- 3 datasets manquants détectés tôt

**CAPTURE D'ÉCRAN** : 
Montrer les 2 fichiers Excel côte à côte (1 capture combinée, 5-6 lignes de chaque)

**VISUEL** : 3 blocs + capture d'écran Excel

---

## Slide 5 : Architecture Médaillon

**Titre** : Architecture Moderne en 3 Couches

**SCHÉMA SIMPLIFIÉ** :

```
         SOURCES (CSV)
              ↓
    🥉 BRONZE - Données brutes
    Stockage tel quel
              ↓
    🥈 SILVER - Transformations
    Calculs métier, filtres, enrichissements
    Format Parquet (performance)
              ↓
    🥇 GOLD - Consolidation
    AZ + AZEC + Enrichissements complets
    Prêt pour BI
```

**Avantages** :
- ✅ Standard industrie
- ✅ Traçabilité complète
- ✅ Qualité progressive

**VISUEL** : Schéma vertical 3 couches avec couleurs

---

## Slide 6 : Les 3 Pipelines Développés

**Titre** : Implémentation Python

**SCHÉMA 3 BLOCS** :

```
┌──────────────────────────┐
│ 1. PTF MOUVEMENTS        │
│ AFN, RES, PTF, capitaux  │
│ ~1.5M contrats           │
└──────────────────────────┘

┌──────────────────────────┐
│ 2. CAPITAUX              │
│ SMP, LCI, indexation     │
│ ~500k lignes             │
└──────────────────────────┘

┌──────────────────────────┐
│ 3. ÉMISSIONS             │
│ Primes One BI            │
│ ~300k lignes             │
└──────────────────────────┘
```

**Architecture du code** :
- Modulaire et réutilisable
- Configuration JSON centralisée
- Logging complet

**VISUEL** : 3 boîtes + arborescence code simplifiée à côté

---

## Slide 7 : SAS vs Python

**Titre** : Comparaison Technique

**TABLEAU SIMPLIFIÉ** :

| | SAS | Python/PySpark |
|---|-----|----------------|
| **Architecture** | Monolithique | Médaillon (3 couches) |
| **Configuration** | Hardcodé | JSON externalisé |
| **Maintenance** | Difficile | Facile |
| **Coûts** | Licences $$$ | Open source |
| **Calculs métier** | ✅ | ✅ **Identiques** |

**Message** : Même résultat, meilleure architecture

**VISUEL** : Tableau avec couleurs (rouge SAS, vert Python)

---

## Slide 8 : Validation en 3 Niveaux

**Titre** : Mon Plan de Tests

**3 ÉTAPES** :

**1. ✅ Structurel** (FAIT)
- Audit 45 datasets
- Vérification logique SAS ↔ Python

**2. 🔄 Fonctionnel** (EN COURS)
- Tests sur 20 visions (2 ans de données)
- Comparaison KPIs (écart attendu < 0.01%)

**3. 📅 Performance** (À VENIR)
- Benchmarks temps d'exécution
- Consommation ressources

**VISUEL** : 3 boîtes avec icônes statut

---

## Slide 9 : Résultats Parité (À COMPLÉTER)

**Titre** : Validation SAS vs Python

**⏳ SLIDE À COMPLÉTER APRÈS VOS TESTS**

**GRAPHIQUE COMPARATIF** :
Barres SAS vs Python pour :
- NBPTF, NBAFN, NBRES
- PRIMES_PTF
- SMP_100, LCI_100

**TABLEAU récapitulatif** :

| Vision | Lignes SAS | Lignes Python | Écart KPIs |
|--------|------------|---------------|------------|
| 202509 | [X] | [Y] | [Z]% |
| ... | ... | ... | ... |

**Verdict** : ✅ Parité validée (< 0.01%)

**VISUEL** : Graphique + petit tableau (5-6 visions)

---

## Slide 10 : Où j'en Suis

**Titre** : État d'Avancement

**TIMELINE VISUELLE** :

```
✅ TERMINÉ                🔄 EN COURS          📅 RESTE À FAIRE

• Analyse SAS           • Tests parité        • Benchmarks
• Documentation         • Validation          • Optimisations
• Recensement             20 visions          • Déploiement
• Architecture                                • Formation
• 3 Pipelines Python                            équipe
• Tests unitaires
```

**Livraison prévue** : Fin février 2025

**VISUEL** : Timeline horizontale avec couleurs (vert/orange/gris)

---

## Slide 11 : Mes Livrables Finaux

**Titre** : Ce que j'ai Produit

**4 CATÉGORIES** :

📚 **Documentation**
- Doc SAS complète
- Doc Python

📊 **Recensements**
- Excel règles métier
- Excel datasets

💻 **Code**
- 3 pipelines Python
- ~5000 lignes de code
- Architecture médaillon

✅ **Validation**
- Tests structurels OK
- Tests fonctionnels en cours

**VISUEL** : Grille 2×2 avec icônes

---

## Slide 12 : Conclusion

**Titre** : En Résumé

**3 points clés** :

**1. Démarche rigoureuse**  
→ Analyser et documenter avant de coder

**2. Architecture moderne**  
→ Médaillon (Bronze/Silver/Gold)

**3. Parité fonctionnelle**  
→ Même résultat, stack moderne (à valider)

**Impact** :
- ✅ Réduction coûts (licences)
- ✅ Stack open source
- ✅ Code maintenable et évolutif

**Merci de votre attention !**

**VISUEL** : 3 icônes + vos coordonnées

---

# 📋 GUIDE CRÉATION VISUELS

## Captures d'écran à faire :
1. **Slide 4** : 2 fichiers Excel côte à côte (6 lignes de chaque)
2. **Slide 9** : Résultats de comparaison (APRÈS tests)

## Schémas PowerPoint à créer :
1. **Slide 2** : 2 colonnes Avant/Après
2. **Slide 3** : Timeline 4 étapes horizontale
3. **Slide 5** : 3 boîtes verticales Bronze/Silver/Gold
4. **Slide 6** : 3 boîtes pipelines
5. **Slide 10** : Timeline avancement
6. **Slide 11** : Grille 2×2 livrables

## Codes couleur :
- **Bronze** : #CD7F32
- **Silver** : #C0C0C0  
- **Gold** : #FFD700
- **Terminé** : #28A745 (vert)
- **En cours** : #FFC107 (orange)
- **À faire** : #6C757D (gris)

---

**🎯 12 slides = 15-20 minutes de présentation parfaite !**

**Principe** : Chaque slide = 1 message clair + 1 visuel percutant
