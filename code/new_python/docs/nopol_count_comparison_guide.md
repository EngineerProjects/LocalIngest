# Guide de Comparaison: NOPOL Count Tracking - SAS vs PySpark

## 📋 Tableau de Référence

| Étape | PySpark Log | SAS Ligne | Description | Attendu |
|-------|-------------|-----------|-------------|---------|
| **0** | `0_AFTER_READ` | L150 | Après lecture PTF16+PTF36 avec filtres | Nombre initial de polices |
| **5** | `5_AFTER_IPFM99` | L183 | Après LEFT join IPFM99 (01099 only) | **= étape 0** (left join n'ajoute pas de lignes) |
| **6** | `6_AFTER_CAPITALS` | L231 | Après extraction capitaux (LBCAPI/MTCAPI) | **= étape 5** (UPDATE ne change pas row count) |
| **7** | `7_BEFORE_MOVEMENTS` | L248 | Avant calcul mouvements | **= étape 6** |
| **8** | `8_AFTER_MOVEMENTS` | L286 | Après calcul AFN/RES/NBPTF | **= étape 7** (UPDATE ne change pas row count) |
| **13** | `13_BEFORE_SEGMENT` | L492 | Avant join segmentation | **= étape 8** |
| **14** | `14_AFTER_SEGMENT` | L502 | Après LEFT join Segment + PT_GEST | **Peut augmenter** si duplicats dans refs! |
| **14b** | `14_BEFORE_DEDUP` | L502 | Après joins, avant dedup | Doit montrer duplicats |
| **15** | `15_AFTER_DEDUP` | L507 | Après `dropDuplicates(['nopol'])` | **Total = Distinct nopol** |

---

## 🔍 Points de Contrôle Critiques

### ✅ Point 1: AFTER_READ = AFTER_IPFM99
```
Si [5_AFTER_IPFM99] > [0_AFTER_READ]
→ ❌ PROBLÈME: IPFM99 a des duplicats sur (cdpole, cdprod, nopol, noint)
→ LEFT join crée un produit cartésien
```

### ✅ Point 2: Tous les UPDATE ne changent pas le count
```
Si [6_AFTER_CAPITALS] ≠ [5_AFTER_IPFM99]
→ ❌ PROBLÈME MAJEUR: extract_capitals modifie le nombre de lignes!

Si [8_AFTER_MOVEMENTS] ≠ [7_BEFORE_MOVEMENTS]
→ ❌ PROBLÈME MAJEUR: calculate_movements modifie le nombre de lignes!
```

### ⚠️ Point 3: Segmentation Join (Zone critique!)
```
Si [14_AFTER_SEGMENT] > [13_BEFORE_SEGMENT]
→ ⚠️  Join segmentation ajoute des lignes
→ Vérifier: df_segment a-t-il des duplicats sur (cprod, reseau)?
→ Vérifier: PT_GEST a-t-il des duplicats sur (ptgst)?

Différence acceptée = [14_AFTER_SEGMENT] - [13_BEFORE_SEGMENT]
```

### ✅ Point 4: Déduplication finale
```
[15_AFTER_DEDUP] doit avoir:
  Total rows = Distinct nopol (les deux doivent être identiques)
  Duplicates = 0

Si Duplicates > 0 après dédup
→ ❌ PROBLÈME CRITIQUE: dropDuplicates(['nopol']) ne fonctionne pas!
```

---

## 📊 Format de Sortie Attendu

```
================================================================================
DIAGNOSTIC: NOPOL COUNT TRACKING (Step-by-step comparison with SAS)
================================================================================
[0_AFTER_READ       ] Total: 71,890 | Distinct nopol: 68,742 | Duplicates: 3,148
[5_AFTER_IPFM99     ] Total: 71,890 | Distinct nopol: 68,742 | Duplicates: 3,148
[6_AFTER_CAPITALS   ] Total: 71,890 | Distinct nopol: 68,742 | Duplicates: 3,148
[7_BEFORE_MOVEMENTS ] Total: 71,890 | Distinct nopol: 68,742 | Duplicates: 3,148
[8_AFTER_MOVEMENTS  ] Total: 71,890 | Distinct nopol: 68,742 | Duplicates: 3,148
[13_BEFORE_SEGMENT  ] Total: 71,890 | Distinct nopol: 68,742 | Duplicates: 3,148
[14_AFTER_SEGMENT   ] Total: 75,234 | Distinct nopol: 68,742 | Duplicates: 6,492  ⚠️ SUSPECT!
[14_BEFORE_DEDUP    ] Total: 75,234 | Distinct nopol: 68,742 | Duplicates: 6,492
[15_AFTER_DEDUP     ] Total: 68,742 | Distinct nopol: 68,742 | Duplicates:     0  ✅ OK
================================================================================
END DIAGNOSTIC: Compare counts above with SAS at each step
================================================================================
```

### Interprétation de l'exemple ci-dessus:
- ✅ Étapes 0-13: Stable (pas de duplicats ajoutés)
- ⚠️ Étape 14: +3,344 duplicats ajoutés après join segmentation
  - → Investiguer df_segment ou PT_GEST pour duplicats
- ✅ Étape 15: Dédup fonctionne (68,742 polices finales)

**Comparaison SAS**: 
- Si SAS a 68,742 polices finales → ✅ **Parité atteinte!**
- Si SAS a moins (ex: 65,594) → ❌ PySpark a encore des polices en trop

---

## 🎯 Plan d'Action Selon les Résultats

### Scénario A: Différence dès AFTER_READ
```
[0_AFTER_READ] PySpark: 75,000 | SAS: 68,000
→ Cause: Filtres bronze ou fichiers sources différents
→ Action: Vérifier reading_config.json et filtres business_rules.json
```

### Scénario B: Différence après IPFM99
```
[5_AFTER_IPFM99] > [0_AFTER_READ]
→ Cause: IPFM99 a des duplicats sur les clés de join
→ Action: Dédupliquer IPFM99 AVANT le join
```

### Scénario C: Différence après Segmentation
```
[14_AFTER_SEGMENT] > [13_BEFORE_SEGMENT]
→ Cause: df_segment ou PT_GEST ont des duplicats
→ Action: Ajouter dropDuplicates sur les tables de référence AVANT join
```

### Scénario D: Dédup finale ne fonctionne pas
```
[15_AFTER_DEDUP] Duplicates > 0
→ Cause: dropDuplicates(['nopol']) ne s'exécute pas correctement
→ Action: Vérifier la syntaxe Spark et ordonner AVANT dropDuplicates
```

---

## 📝 Prochaine Étape

1. **Exécuter le pipeline AZ** avec le logging activé
2. **Copier les logs** dans ce fichier (section ci-dessous)
3. **Comparer** avec SAS ligne par ligne
4. **Identifier** à quelle étape la divergence apparaît
5. **Appliquer** le plan d'action correspondant

---

## 📊 Logs PySpark (À remplir après exécution)

```
[Coller ici les logs de l'exécution PySpark]
```

---

## 📊 Références SAS (À remplir)

```
[Coller ici les counts SAS à chaque étape si disponibles]
```
