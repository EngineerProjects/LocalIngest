# Guide de Conversion des Fichiers SAS Téléchargés

## 🎯 Problèmes à Résoudre

Après téléchargement depuis SAS, vous avez :
1. ❌ **Séparateur `;`** au lieu de `|`
2. ❌ **Encodage LATIN9** avec caractères "bizarres" (`é` → `?`)
3. ⚠️ **Fichiers énormes** (40M+ lignes)

---

## ✅ Solution Automatique

### **Script Optimisé pour Gros Fichiers**

```bash
cd /home/amiche/Downloads/code

# Convertir tous les CSV téléchargés
python workenv/fix_csv_separator.py ~/Downloads/*.csv
```

**Avantages** :
- ✅ Traite fichiers ligne par ligne (streaming)
- ✅ Mémoire constante (pas de crash sur 40M lignes)
- ✅ Gère automatiquement LATIN9
- ✅ Affiche progression tous les 1M lignes
- ✅ Remplace caractères invalides automatiquement

---

## 📋 Procédure Complète

### **1. Télécharger depuis SAS**

Vous avez déjà fait ça ! ✓

Fichiers obtenus :
- ✓ segmentprdt_202509.csv
- ✓ indices.csv
- ✓ basecli_inv.csv (40M+ lignes)
- ✓ histo_note_risque.csv (40M+ lignes)
- ✓ do_dest_202110.csv
- ✓ table_segmentation_azec_mml.csv
- ❌ rf_fr1_prm_dtl_midcorp_m_202509.csv (One BI - indisponible)

---

### **2. Convertir les Séparateurs**

```bash
# Tous les fichiers d'un coup
python workenv/fix_csv_separator.py ~/Downloads/*.csv

# Ou un par un si vous préférez
python workenv/fix_csv_separator.py ~/Downloads/segmentprdt_202509.csv
python workenv/fix_csv_separator.py ~/Downloads/basecli_inv.csv
# etc.
```

**Résultat attendu** :
```
📄 Conversion: basecli_inv.csv
   Encodage: latin-9
   ; -> |
   ⏳ 1,000,000 lignes (85.3 MB)
   ⏳ 2,000,000 lignes (170.6 MB)
   ...
   ⏳ 40,000,000 lignes (3418.2 MB)
   ✓ Terminé: 40,123,456 lignes (3432.1 MB)
```

---

### **3. Copier vers le Datalake**

```bash
# Fichiers reference
cp ~/Downloads/segmentprdt_202509.csv /workspace/datalake/bronze/ref/
cp ~/Downloads/indices.csv /workspace/datalake/bronze/ref/
cp ~/Downloads/basecli_inv.csv /workspace/datalake/bronze/ref/
cp ~/Downloads/histo_note_risque.csv /workspace/datalake/bronze/ref/
cp ~/Downloads/do_dest_202110.csv /workspace/datalake/bronze/ref/
cp ~/Downloads/table_segmentation_azec_mml.csv /workspace/datalake/bronze/ref/
```

---

### **4. Générer le Fichier One BI Manquant**

```bash
# Utiliser le générateur pour créer données de test
python workenv/data_generator.py

# Copier résultat
cp workenv/bronze/monthly/rf_fr1_prm_dtl_midcorp_m_202509.csv \
   /workspace/datalake/bronze/2025/09/
```

---

## 🔍 Vérification Encodage

### **Problème : Caractères `?` au lieu de `é`, `à`, etc.**

**Cause** : Éditeur texte ouvre en UTF-8, fichier est en LATIN9

**Solution PySpark** :
```python
# Votre config reading_config.json le gère déjà !
"read_options": {
    "encoding": "LATIN9"  # ✓ Correct
}
```

**PySpark convertira automatiquement** :
```
LATIN9 → UTF-8 interne
```

### **Test Rapide**

```bash
# Vérifier que PySpark lit bien
python -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('test').getOrCreate()
df = spark.read.csv(
    '/workspace/datalake/bronze/ref/segmentprdt_202509.csv',
    sep='|',
    header=True,
    encoding='LATIN9'
)
print(f'Lignes: {df.count()}')
df.show(5)
"
```

---

## ⚠️ Options du Script

### **Garder l'original (backup)**

```bash
python workenv/fix_csv_separator.py --keep-original ~/Downloads/fichier.csv
# Crée: fichier.csv.bak
```

### **Encodage personnalisé**

```bash
# Si encodage différent
python workenv/fix_csv_separator.py --encoding utf-8 ~/Downloads/fichier.csv
```

---

## 🎯 Résumé

| Étape | Commande | Durée (40M lignes) |
|-------|----------|-------------------|
| 1. Télécharger SAS | Manuel | 5-10 min |
| 2. Convertir ; → \| | `fix_csv_separator.py` | ~2-3 min |
| 3. Copier datalake | `cp` | 1 min |
| 4. Tester PySpark | Pipeline | - |

**Total : ~10-15 minutes pour préparer les données** ✅
