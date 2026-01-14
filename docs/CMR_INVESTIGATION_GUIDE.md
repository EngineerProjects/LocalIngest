# Investigation Produit CMR dans AZEC - Guide Manuel

Guide pratique pour investiguer l'apparition du produit CMR dans les données AZEC.

---

## 🎯 Objectif

Déterminer si **CMR** provient :
- ❌ D'une erreur dans les fichiers sources POLIC_CU
- ❌ D'un problème de traitement Python
- ✅ Identifier la source exacte du problème

---

## 📋 Étape 1: Vérifier Données Bronze POLIC_CU

### Query PySpark

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lit

# Initialiser Spark
spark = SparkSession.builder.appName("CMR_Investigation").getOrCreate()

# Choisir une vision récente (adapter selon disponibilité)
vision = "202312"  # MODIFIER selon vos données

# Lire fichier bronze POLIC_CU
bronze_path = f"/home/amiche/Projects/LocalIngest/data/bronze/polic_cu/polic_cu_{vision}.parquet"
df_bronze = spark.read.parquet(bronze_path)

print(f"=== ANALYSE BRONZE POLIC_CU - Vision {vision} ===\n")

# 1. Lister TOUS les produits
print("1. Distribution des produits dans POLIC_CU:")
df_bronze.groupBy("produit").count().orderBy(col("count").desc()).show(100, False)

# 2. Chercher spécifiquement CMR
print("\n2. Recherche produit CMR:")
df_cmr = df_bronze.filter(col("produit") == "CMR")
nb_cmr = df_cmr.count()
print(f"   Nombre de polices CMR: {nb_cmr}")

if nb_cmr > 0:
    print("\n   ⚠️ CMR TROUVÉ DANS BRONZE! Détails:")
    df_cmr.select("police", "produit", "duree", "etatpol", "effetpol", "datfin").show(20, False)
else:
    print("   ✅ Aucun CMR dans bronze")

# 3. Chercher CNR pour comparaison
print("\n3. Pour comparaison - Produit CNR:")
df_cnr = df_bronze.filter(col("produit") == "CNR")
nb_cnr = df_cnr.count()
print(f"   Nombre de polices CNR: {nb_cnr}")

if nb_cnr > 0:
    print("   Échantillon CNR:")
    df_cnr.select("police", "produit", "duree", "etatpol", "effetpol", "datfin").show(10, False)
```

### Commande Shell Alternative

```bash
# Si les données sont en CSV
cd /home/amiche/Projects/LocalIngest/data/bronze/polic_cu

# Chercher CMR
grep -i "CMR" polic_cu_202312.csv | head -20

# Compter occurrences
grep -i "CMR" polic_cu_202312.csv | wc -l
```

---

## 📋 Étape 2: Vérifier Après Filtres AZEC

### Query PySpark

```python
# Lire données silver AZEC (après filtres)
silver_path = f"/home/amiche/Projects/LocalIngest/data/silver/azec_ptf/azec_ptf_{vision}.parquet"

try:
    df_silver = spark.read.parquet(silver_path)
    
    print(f"\n=== ANALYSE SILVER AZEC_PTF - Vision {vision} ===\n")
    
    # 1. Distribution produits dans silver
    print("1. Distribution des produits dans AZEC_PTF (après filtres):")
    df_silver.groupBy("cdprod").count().orderBy(col("count").desc()).show(50, False)
    
    # 2. Chercher CMR dans silver
    print("\n2. Recherche CMR dans AZEC_PTF:")
    df_cmr_silver = df_silver.filter(col("cdprod") == "CMR")
    nb_cmr_silver = df_cmr_silver.count()
    print(f"   Nombre de CMR après filtres: {nb_cmr_silver}")
    
    if nb_cmr_silver > 0:
        print("\n   🔴 CMR PRÉSENT APRÈS FILTRES! (PROBLÈME)")
        print("   Détails:")
        df_cmr_silver.select("nopol", "cdprod", "duree", "etatpol", "nbptf", "nbafn").show(20, False)
    else:
        print("   ✅ CMR correctement filtré (attendu)")
        
except Exception as e:
    print(f"   ⚠️ Fichier silver non trouvé: {e}")
```

---

## 📋 Étape 3: Comparer Profils CMR vs CNR

### Query PySpark

```python
# Uniquement si CMR trouvé dans bronze
if nb_cmr > 0:
    print("\n=== COMPARAISON CMR vs CNR ===\n")
    
    # Profil CMR
    print("Profil CMR:")
    print("  - Par DUREE:")
    df_cmr.groupBy("duree").count().show()
    print("  - Par ETATPOL:")
    df_cmr.groupBy("etatpol").count().show()
    print("  - Par CODECOAS:")
    df_cmr.groupBy("codecoas").count().show()
    
    # Profil CNR
    print("\nProfil CNR (pour comparaison):")
    print("  - Par DUREE:")
    df_cnr.groupBy("duree").count().show()
    print("  - Par ETATPOL:")
    df_cnr.groupBy("etatpol").count().show()
    print("  - Par CODECOAS:")
    df_cnr.groupBy("codecoas").count().show()
    
    # Statistiques
    print("\n📊 Statistiques comparatives:")
    print(f"   CMR: {nb_cmr} polices")
    print(f"   CNR: {nb_cnr} polices")
    print(f"   Ratio: {(nb_cmr/(nb_cnr+nb_cmr)*100):.2f}% CMR vs {(nb_cnr/(nb_cnr+nb_cmr)*100):.2f}% CNR")
```

---

## 📋 Étape 4: Vérifier Historique (Plusieurs Visions)

### Query PySpark

```python
# Vérifier sur plusieurs visions
visions = ["202310", "202311", "202312", "202401", "202402"]  # ADAPTER

print("\n=== HISTORIQUE CMR SUR PLUSIEURS VISIONS ===\n")
print("Vision    | CMR Bronze | CMR Silver | CNR Bronze")
print("----------|------------|------------|------------")

for v in visions:
    try:
        df_b = spark.read.parquet(f"/home/amiche/Projects/LocalIngest/data/bronze/polic_cu/polic_cu_{v}.parquet")
        nb_cmr_b = df_b.filter(col("produit") == "CMR").count()
        nb_cnr_b = df_b.filter(col("produit") == "CNR").count()
        
        try:
            df_s = spark.read.parquet(f"/home/amiche/Projects/LocalIngest/data/silver/azec_ptf/azec_ptf_{v}.parquet")
            nb_cmr_s = df_s.filter(col("cdprod") == "CMR").count()
        except:
            nb_cmr_s = "N/A"
            
        print(f"{v}   | {nb_cmr_b:10} | {str(nb_cmr_s):10} | {nb_cnr_b:10}")
    except Exception as e:
        print(f"{v}   | Fichier non trouvé")

print("\n💡 Si CMR apparaît sur plusieurs visions → Problème systématique source")
print("💡 Si CMR seulement sur certaines visions → Incident ponctuel")
```

---

## 📋 Étape 5: Vérifier Fichiers Sources CSV (si applicable)

### Commandes Shell

```bash
cd /home/amiche/Projects/LocalIngest/data/raw/azec

# Lister fichiers disponibles
ls -lh polic_cu*.csv

# Chercher CMR dans fichier source CSV
echo "=== Recherche CMR dans fichiers CSV ==="
for file in polic_cu*.csv; do
    count=$(grep -c "CMR" "$file" 2>/dev/null || echo 0)
    if [ "$count" -gt 0 ]; then
        echo "$file: $count occurrences de CMR"
        echo "Premières lignes:"
        grep "CMR" "$file" | head -3
    fi
done

# Chercher CNR pour comparaison
echo ""
echo "=== CNR pour comparaison ==="
grep "CNR" polic_cu_202312.csv | wc -l
grep "CNR" polic_cu_202312.csv | head -3
```

---

## 📋 Interprétation des Résultats

### Cas 1: CMR dans Bronze, PAS dans Silver ✅

**Diagnostic** : Filtres fonctionnent correctement  
**Action** : Aucune - CMR est bien éliminé

### Cas 2: CMR dans Bronze ET dans Silver 🔴

**Diagnostic** : Problème de filtre Python  
**Action** : Ajouter filtre explicite `produit != 'CMR'`

### Cas 3: Pas de CMR du tout ✅

**Diagnostic** : Problème résolu ou inexistant  
**Action** : Aucune

### Cas 4: CMR = Profil identique à CNR ⚠️

**Diagnostic** : Forte probabilité que CMR = typo de CNR  
**Action** : Corriger données source ou mapper CMR → CNR

---

## 🔧 Actions Correctives Selon Diagnostic

### Si CMR trouvé dans fichier source CSV

```bash
# Option 1: Correction manuelle du CSV (BACKUP d'abord!)
cd /home/amiche/Projects/LocalIngest/data/raw/azec

# Backup
cp polic_cu_202312.csv polic_cu_202312.csv.backup

# Remplacer CMR par CNR (SI VALIDÉ PAR MÉTIER)
sed -i 's/,CMR,/,CNR,/g' polic_cu_202312.csv

# Vérifier
echo "Vérification après correction:"
grep -c "CMR" polic_cu_202312.csv
grep -c "CNR" polic_cu_202312.csv
```

### Si CMR doit être filtré en Python

**Ajouter dans business_rules.json** :

```json
{
  "type": "not_in",
  "column": "produit",
  "values": ["CMR"],
  "description": "Exclude CMR (invalid product - data quality issue)"
}
```

### Si CMR doit être mappé vers CNR

**Ajouter dans azec_processor.py avant filtres** :

```python
# Step 0.5: Fix data quality - Map CMR to CNR
if 'produit' in df.columns:
    df = df.withColumn('produit',
        when(col('produit') == 'CMR', lit('CNR'))
        .otherwise(col('produit'))
    )
    self.logger.info("Applied data quality fix: CMR → CNR")
```

---

## 📝 Rapport à Compléter

```
=== RÉSULTATS INVESTIGATION CMR ===

Date investigation: __________
Visions analysées: __________

1. CMR dans bronze POLIC_CU?        OUI / NON
   - Nombre de polices:             __________
   - % du portefeuille:             __________

2. CMR dans silver AZEC_PTF?        OUI / NON
   - Nombre après filtres:          __________

3. Profil CMR similaire à CNR?      OUI / NON

4. CMR sur plusieurs visions?       OUI / NON
   - Visions concernées:            __________

5. Source du problème:
   [ ] Fichier CSV source POLIC_CU
   [ ] Extraction base AZEC legacy
   [ ] Problème de filtre Python
   [ ] Autre: __________

6. Action prise:
   [ ] Correction fichier source
   [ ] Ajout filtre CMR
   [ ] Mapping CMR → CNR
   [ ] Escalade équipe métier
   [ ] Aucune (pas de CMR trouvé)

Notes:
_________________________________
_________________________________
_________________________________
```

---

## Script Complet d'Investigation

**Fichier**: `investigate_cmr.py`

```python
#!/usr/bin/env python3
"""
Script d'investigation CMR dans AZEC
Usage: python investigate_cmr.py <vision>
Example: python investigate_cmr.py 202312
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def investigate_cmr(vision):
    spark = SparkSession.builder.appName("CMR_Investigation").getOrCreate()
    
    print(f"\n{'='*60}")
    print(f"INVESTIGATION CMR - Vision {vision}")
    print(f"{'='*60}\n")
    
    # Bronze
    print("1. BRONZE POLIC_CU:")
    try:
        df_bronze = spark.read.parquet(f"data/bronze/polic_cu/polic_cu_{vision}.parquet")
        cmr_count = df_bronze.filter(col("produit") == "CMR").count()
        cnr_count = df_bronze.filter(col("produit") == "CNR").count()
        
        print(f"   CMR: {cmr_count:,} polices")
        print(f"   CNR: {cnr_count:,} polices")
        
        if cmr_count > 0:
            print(f"   ⚠️ CMR PRÉSENT dans bronze!")
        else:
            print(f"   ✅ Pas de CMR dans bronze")
    except Exception as e:
        print(f"   ❌ Erreur: {e}")
    
    # Silver
    print("\n2. SILVER AZEC_PTF:")
    try:
        df_silver = spark.read.parquet(f"data/silver/azec_ptf/azec_ptf_{vision}.parquet")
        cmr_silver = df_silver.filter(col("cdprod") == "CMR").count()
        
        print(f"   CMR après filtres: {cmr_silver:,}")
        
        if cmr_silver > 0:
            print(f"   🔴 CMR TOUJOURS PRÉSENT après filtres!")
        else:
            print(f"   ✅ CMR correctement filtré")
    except Exception as e:
        print(f"   ⚠️ Silver non trouvé: {e}")
    
    print(f"\n{'='*60}\n")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python investigate_cmr.py <vision>")
        print("Example: python investigate_cmr.py 202312")
        sys.exit(1)
    
    investigate_cmr(sys.argv[1])
```

**Exécution** :
```bash
cd /home/amiche/Projects/LocalIngest/code/new_python
python investigate_cmr.py 202312
```
