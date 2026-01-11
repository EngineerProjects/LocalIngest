# Row Count Diagnostic Guide - Server Verification

## Objectif
Comparer les sorties SAS (85,014 lignes) et Python (88,900 lignes) pour identifier la source des +3,886 lignes supplémentaires.

---

## 🎯 Stratégie d'Investigation

### Hypothèses principales (basées sur l'analyse du code):
1. **Duplicatas dans les joins IPFM** (déjà fixé - à vérifier)
2. **Duplicatas dans DO_DEST** (déjà fixé - à vérifier)
3. **Différences dans les filtres business** (segment, cmarch, etc.)
4. **Différences dans l'union AZ + AZEC**

---

## 📊 Scripts de Vérification

### 1. Comparaison Globale

```python
# Sur le serveur PySpark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("RowCountDebug").getOrCreate()

# Charger les données
sas_df = spark.read.format("delta").load("path/to/sas/output/ptf_mvt_202509")
python_df = spark.read.format("delta").load("abfs://shared@azfrdatalab.dfs.core.windows.net/ABR/P4D/ADC/DATAMARTS/CONSTRUCTION/gold/2025/09/ptf_mvt_202509")

print(f"SAS rows: {sas_df.count():,}")
print(f"Python rows: {python_df.count():,}")
print(f"Difference: {python_df.count() - sas_df.count():,}")
```

---

### 2. Vérification des Duplicatas par Clé

```python
# Identifier les duplicatas dans Python
from pyspark.sql.functions import count, col

# Clé primaire supposée: (nopol, cdprod)
python_dupes = python_df.groupBy("nopol", "cdprod").agg(
    count("*").alias("count")
).filter(col("count") > 1)

print(f"Nombre de clés dupliquées (nopol, cdprod): {python_dupes.count()}")
print("\nTop 10 duplicatas:")
python_dupes.orderBy(col("count").desc()).show(10)

# Comparer avec SAS
sas_dupes = sas_df.groupBy("nopol", "cdprod").agg(
    count("*").alias("count")
).filter(col("count") > 1)

print(f"\nSAS - Nombre de clés dupliquées: {sas_dupes.count()}")
```

---

### 3. Comparaison par Source (AZ vs AZEC)

```python
# Compter par source
print("=== Python ===")
python_df.groupBy("dircom").count().orderBy("dircom").show()

print("\n=== SAS ===")
sas_df.groupBy("dircom").count().orderBy("dircom").show()

# Vérifier CDPOLE
print("\n=== Python - Par Pole ===")
python_df.groupBy("cdpole").count().orderBy("cdpole").show()

print("\n=== SAS - Par Pole ===")
sas_df.groupBy("cdpole").count().orderBy("cdpole").show()
```

---

### 4. Identifier les Lignes Uniques à Python

```python
# Trouver les lignes présentes dans Python mais pas dans SAS
# (nécessite une clé unique - utilisez nopol+cdprod ou créez un hash)

from pyspark.sql.functions import concat_ws, md5

# Créer une clé unique
python_keyed = python_df.withColumn("row_key", md5(concat_ws("|", 
    col("nopol"), col("cdprod"), col("vision"), col("cdpole")
)))

sas_keyed = sas_df.withColumn("row_key", md5(concat_ws("|",
    col("nopol"), col("cdprod"), col("vision"), col("cdpole")  
)))

# Anti-join: lignes dans Python mais pas dans SAS
python_only = python_keyed.join(sas_keyed, on="row_key", how="left_anti")

print(f"Lignes uniquement dans Python: {python_only.count()}")

# Analyser ces lignes
print("\n=== Profil des lignes en excès ===")
python_only.groupBy("cdpole", "segment2", "cdnatp").count().orderBy("count", ascending=False).show(20)

# Sauvegarder pour analyse
python_only.write.format("delta").mode("overwrite").save("path/to/debug/python_only_rows")
```

---

### 5. Vérification des Filtres Business

```python
# Vérifier les filtres appliqués
print("=== Vérification CMARCH (doit être '6' pour construction) ===")
print(f"Python - Non '6': {python_df.filter(col('cmarch') != '6').count()}")
print(f"SAS - Non '6': {sas_df.filter(col('cmarch') != '6').count()}")

print("\n=== Vérification SEGMENT (construction uniquement) ===")
python_df.groupBy("segment2").count().orderBy("count", ascending=False).show()
sas_df.groupBy("segment2").count().orderBy("count", ascending=False).show()

print("\n=== Vérification CDNATP (R/C pour construction) ===")
python_df.groupBy("cdnatp").count().show()
sas_df.groupBy("cdnatp").count().show()
```

---

### 6. Vérification des Joins IPFM (Special Products)

```python
# Vérifier si les colonnes IPFM causent des duplicatas
print("=== Policies avec ACTPRIN (de IPFM) ===")
python_with_actprin = python_df.filter(col("actprin").isNotNull())
sas_with_actprin = sas_df.filter(col("actprin").isNotNull())

print(f"Python - Avec ACTPRIN: {python_with_actprin.count()}")
print(f"SAS - Avec ACTPRIN: {sas_with_actprin.count()}")

# Chercher duplicatas sur ces lignes spécifiquement
python_actprin_dupes = python_with_actprin.groupBy("nopol", "cdprod").agg(
    count("*").alias("count")
).filter(col("count") > 1)

print(f"Duplicatas dans Python (avec ACTPRIN): {python_actprin_dupes.count()}")
```

---

### 7. Comparaison des Sources (Silver Layer)

```python
# Vérifier les inputs AVANT consolidation
az_silver = spark.read.format("delta").load("abfs://.../silver/2025/09/mvt_const_ptf_202509")
azec_silver = spark.read.format("delta").load("abfs://.../silver/2025/09/azec_ptf_202509")

print(f"AZ Silver: {az_silver.count():,} rows")
print(f"AZEC Silver: {azec_silver.count():,} rows")
print(f"Total: {az_silver.count() + azec_silver.count():,} rows")

# Vérifier duplicatas dans AZ
az_dupes = az_silver.groupBy("nopol").agg(count("*").alias("count")).filter(col("count") > 1)
print(f"AZ duplicatas (nopol): {az_dupes.count()}")

# Vérifier duplicatas dans AZEC
azec_dupes = azec_silver.groupBy("police").agg(count("*").alias("count")).filter(col("count") > 1)
print(f"AZEC duplicatas (police): {azec_dupes.count()}")
```

---

## 🔍 Analyse Manuelle Rapide

### Option A: Échantillonnage de Duplicatas

```python
# Prendre un exemple de nopol dupliqué
sample_dupes = python_dupes.limit(5).select("nopol", "cdprod").collect()

for row in sample_dupes:
    nopol, cdprod = row.nopol, row.cdprod
    print(f"\n=== Analyzing {nopol} / {cdprod} ===")
    
    # Afficher toutes les lignes pour ce nopol/cdprod
    python_df.filter((col("nopol") == nopol) & (col("cdprod") == cdprod)).show(truncate=False)
    
    # Voir les différences entre les lignes
    cols_to_check = ["cdpole", "noint", "actprin", "cdactconst2", "destinat", "segment2"]
    python_df.filter(
        (col("nopol") == nopol) & (col("cdprod") == cdprod)
    ).select("nopol", "cdprod", *cols_to_check).show(truncate=False)
```

### Option B: Export pour Analyse Excel

```python
# Exporter un échantillon pour analyse manuelle
python_sample = python_only.limit(1000)
python_sample.toPandas().to_excel("/tmp/python_extra_rows_sample.xlsx", index=False)

# Ou sauvegarder en CSV
python_sample.write.format("csv").option("header", "true").mode("overwrite").save("/tmp/python_extra_rows.csv")
```

---

## 🎯 Zones à Vérifier en Priorité

Sur la base de notre analyse de code, vérifiez dans cet ordre:

### 1. **IPFM Special Products** (Priorité HAUTE)
```python
# Vérifier si les fixes de deduplication ont fonctionné
# Chercher les nopol présents dans IPFM0024/63/99 ET dupliqués
ipfm_policies = spark.read.format("delta").load("path/to/ipfm_data")
ipfm_dupes = ipfm_policies.groupBy("nopol", "cdprod").agg(count("*").alias("count")).filter(col("count") > 1)
print(f"IPFM avec duplicatas: {ipfm_dupes.count()}")
```

### 2. **DO_DEST Reference** (Priorité HAUTE)
```python
# Vérifier si DO_DEST a des duplicatas
do_dest = spark.read.format("delta").load("path/to/do_dest")
do_dest_dupes = do_dest.groupBy("nopol").agg(count("*").alias("count")).filter(col("count") > 1)
print(f"DO_DEST avec duplicatas: {do_dest_dupes.count()}")
```

### 3. **Client Tables** (Priorité MOYENNE)
```python
# Vérifier CLIENT1/CLIENT3
client1 = spark.read.format("delta").load("path/to/client1")
client3 = spark.read.format("delta").load("path/to/client3")

print(f"CLIENT1 dupes: {client1.groupBy('noclt').agg(count('*').alias('c')).filter(col('c')>1).count()}")
print(f"CLIENT3 dupes: {client3.groupBy('noclt').agg(count('*').alias('c')).filter(col('c')>1).count()}")
```

---

## 📋 Checklist de Vérification

- [ ] Compter les lignes Python vs SAS
- [ ] Identifier les duplicatas par (nopol, cdprod)
- [ ] Comparer la distribution AZ vs AZEC
- [ ] Identifier les lignes uniques à Python
- [ ] Analyser le profil des lignes en excès (segment, cdpole, etc.)
- [ ] Vérifier les filtres business (cmarch='6', etc.)
- [ ] Tracer quelques exemples de duplicatas
- [ ] Vérifier les sources IPFM pour duplicatas
- [ ] Vérifier DO_DEST pour duplicatas
- [ ] Comparer les Silver layers (AZ + AZEC)

---

## 🚨 Si Vous Trouvez des Duplicatas

1. **Noter les colonnes qui diffèrent** entre les lignes dupliquées
2. **Tracer en arrière** quelle transformation a créé le duplicata
3. **Vérifier si le SAS a le même duplicata** ou un filtre manquant
4. **Corriger** en ajoutant `.dropDuplicates()` au bon endroit

---

## 💡 Conseils

- Utilisez `.cache()` sur les DataFrames que vous réutilisez
- Sauvegardez les résultats intermédiaires pour ne pas recalculer
- Commencez par un échantillon (`.limit(10000)`) pour les tests rapides
- Utilisez `.explain()` si les requêtes sont lentes
- Comparez d'abord les COMPTES avant d'analyser ligne par ligne

---

## 📞 Résultats Attendus

Une fois l'analyse terminée, vous devriez pouvoir identifier:
1. **Combien** de lignes sont dupliquées (vs uniques)
2. **Où** les duplicatas apparaissent (quel join/transformation)
3. **Pourquoi** SAS n'a pas ces duplicatas (filtre manquant? dedup?)
4. **Comment** corriger le code Python

Bonne chance! 🚀
