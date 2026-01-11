# Line-by-Line Comparison Script (PySpark)

## Objectif
Comparer les sorties SAS vs Python ligne par ligne pour identifier:
1. Lignes identiques
2. Lignes différentes (mêmes clés, valeurs différentes)
3. Lignes manquantes (dans SAS mais pas Python)
4. Lignes en excès (dans Python mais pas SAS)

---

## Script 1: Comparaison par Clé (nopol, cdprod)

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lit, when, concat_ws, md5

spark = SparkSession.builder.appName("RowComparison").getOrCreate()

# Charger les données
sas_path = "abfs://.../sas_output/ptf_mvt_202509"  # ← CHANGEZ CE CHEMIN
python_path = "abfs://shared@azfrdatalab.dfs.core.windows.net/ABR/P4D/ADC/DATAMARTS/CONSTRUCTION/gold/2025/09/ptf_mvt_202509"

sas_df = spark.read.format("delta").load(sas_path)
python_df = spark.read.format("delta").load(python_path)

print(f"SAS rows: {sas_df.count():,}")
print(f"Python rows: {python_df.count():,}")

# Vérifier les clés (nopol, cdprod) uniques
sas_keys = sas_df.select("nopol", "cdprod").distinct()
python_keys = python_df.select("nopol", "cdprod").distinct()

print(f"\nSAS unique (nopol, cdprod): {sas_keys.count():,}")
print(f"Python unique (nopol, cdprod): {python_keys.count():,}")

# Comparer les clés
common_keys = sas_keys.intersect(python_keys)
sas_only = sas_keys.subtract(python_keys)
python_only = python_keys.subtract(sas_keys)

print(f"\nCommon keys: {common_keys.count():,}")
print(f"SAS only: {sas_only.count():,}")
print(f"Python only: {python_only.count():,}")

# Afficher quelques exemples
if sas_only.count() > 0:
    print("\n=== Sample keys ONLY in SAS ===")
    sas_only.show(10, truncate=False)

if python_only.count() > 0:
    print("\n=== Sample keys ONLY in Python ===")
    python_only.show(10, truncate=False)
```

---

## Script 2: Vérifier Overlap AZ/AZEC

```python
# Vérifier s'il y a des nopol en commun entre AZ et AZEC
# Cela expliquera si le dedup a un impact ou non

# Charger AZ et AZEC silver
az_df = spark.read.format("delta").load("abfs://.../silver/2025/09/mvt_const_ptf_202509")
azec_df = spark.read.format("delta").load("abfs://.../silver/2025/09/azec_ptf_202509")

print(f"AZ rows: {az_df.count():,}")
print(f"AZEC rows: {azec_df.count():,}")

# Nopol uniques
az_nopol = az_df.select("nopol").distinct()
azec_nopol = azec_df.select(col("police").alias("nopol")).distinct()

print(f"\nAZ unique nopol: {az_nopol.count():,}")
print(f"AZEC unique police: {azec_nopol.count():,}")

# Overlap
overlap = az_nopol.intersect(azec_nopol)
print(f"Overlapping nopol (AZ ∩ AZEC): {overlap.count():,}")

if overlap.count() > 0:
    print("\n⚠️ WARNING: There ARE overlapping nopol between AZ and AZEC!")
    print("The dedup should have an effect. If row count didn't change:")
    print("  → Check if dedup was applied AFTER all transformations")
    print("  → Check if orderBy is deterministic")
    overlap.show(20, truncate=False)
else:
    print("\n✓ No overlapping nopol between AZ and AZEC")
    print("  → Dedup has NO effect (AZ and AZEC are disjoint)")
    print("  → Row count difference is NOT from union duplicates")

# Total if no overlap
expected_total = az_df.count() + azec_df.count() - overlap.count()
print(f"\nExpected total after dedup: {expected_total:,}")
```

---

## Script 3: Comparaison de Contenu (Hash)

```python
# Pour les clés communes, vérifier si les VALEURS sont identiques

# Ajouter un hash de toutes les colonnes
def add_row_hash(df, key_cols):
    """Ajouter un hash de toutes les valeurs pour comparaison."""
    all_cols = [c for c in df.columns if c not in key_cols]
    hash_expr = md5(concat_ws("|", *[col(c).cast("string") for c in all_cols]))
    return df.withColumn("row_hash", hash_expr)

# Ajouter hash
sas_hashed = add_row_hash(sas_df, ["nopol", "cdprod"])
python_hashed = add_row_hash(python_df, ["nopol", "cdprod"])

# Joindre sur les clés
comparison = sas_hashed.select("nopol", "cdprod", col("row_hash").alias("sas_hash")) \
    .join(
        python_hashed.select("nopol", "cdprod", col("row_hash").alias("python_hash")),
        on=["nopol", "cdprod"],
        how="outer"
    )

# Classifier
comparison = comparison.withColumn(
    "status",
    when(col("sas_hash").isNull(), lit("PYTHON_ONLY"))
    .when(col("python_hash").isNull(), lit("SAS_ONLY"))
    .when(col("sas_hash") == col("python_hash"), lit("IDENTICAL"))
    .otherwise(lit("DIFFERENT_VALUES"))
)

# Compter
status_counts = comparison.groupBy("status").count().orderBy("status")
print("\n=== Comparison Results ===")
status_counts.show(truncate=False)

# Détails des différences
different = comparison.filter(col("status") == "DIFFERENT_VALUES")
if different.count() > 0:
    print(f"\n{different.count():,} rows with SAME keys but DIFFERENT values")
    different.show(20, truncate=False)
    
    # Sauvegarder pour analyse détaillée
    different.write.format("delta").mode("overwrite").save("/tmp/different_values_keys")
```

---

## Script 4: Analyse Détaillée des Différences

```python
# Pour les lignes avec différences, identifier QUELLES colonnes diffèrent

from pyspark.sql.functions import struct, to_json

# Prendre les clés avec différences
diff_keys = different.select("nopol", "cdprod")

# Joindre les DataFrames complets
sas_full = sas_df.join(diff_keys, on=["nopol", "cdprod"], how="inner")
python_full = python_df.join(diff_keys, on=["nopol", "cdprod"], how="inner")

# Comparer colonne par colonne (exemple pour quelques colonnes)
sample_key = diff_keys.limit(1).collect()[0]
nopol_sample = sample_key.nopol
cdprod_sample = sample_key.cdprod

print(f"\n=== Detailed comparison for nopol={nopol_sample}, cdprod={cdprod_sample} ===")

sas_row = sas_df.filter((col("nopol") == nopol_sample) & (col("cdprod") == cdprod_sample)).first()
python_row = python_df.filter((col("nopol") == nopol_sample) & (col("cdprod") == cdprod_sample)).first()

print("\nColumn-by-column comparison:")
for col_name in sorted(sas_df.columns):
    sas_val = getattr(sas_row, col_name, None)
    python_val = getattr(python_row, col_name, None)
    
    if sas_val != python_val:
        print(f"  ❌ {col_name:30s}: SAS={sas_val!r:30s} | Python={python_val!r}")
    # Optionnel: afficher aussi les colonnes identiques
    # else:
    #     print(f"  ✓ {col_name:30s}: {sas_val!r}")
```

---

## Script 5: Export pour Excel

```python
# Exporter un échantillon pour analyse manuelle

# Top 100 différences
sample_diff = different.limit(100)

# Récupérer les lignes complètes
sas_sample = sas_df.join(sample_diff.select("nopol", "cdprod"), on=["nopol", "cdprod"])
python_sample = python_df.join(sample_diff.select("nopol", "cdprod"), on=["nopol", "cdprod"])

# Ajouter une colonne source
sas_sample = sas_sample.withColumn("source", lit("SAS"))
python_sample = python_sample.withColumn("source", lit("PYTHON"))

# Union pour côte-à-côte
combined = sas_sample.unionByName(python_sample)

# Sauvegarder
combined.toPandas().to_excel("/tmp/sas_vs_python_differences.xlsx", index=False)
print("\n✓ Exported to /tmp/sas_vs_python_differences.xlsx")

# Ou CSV
combined.write.format("csv").option("header", "true").mode("overwrite").save("/tmp/differences_csv")
print("✓ Exported to /tmp/differences_csv/")
```

---

## Résultats Attendus

### Scénario A: Même nombre de lignes (88,900 = 88,900)
→ Vérifiez si les **valeurs** sont identiques ou différentes  
→ Utilisez Script 3 pour identifier colonnes avec différences

### Scénario B: Lignes supplémentaires dans Python
→ Utilisez Script 1 pour identifier les clés Python-only  
→ Tracez en arrière: d'où viennent ces nopol?

### Scénario C: Lignes manquantes dans Python
→ Utilisez Script 1 pour identifier les clés SAS-only  
→ Vérifiez les filtres business (cmarch, segment, etc.)

---

## 💡 Conseil

Commencez par **Script 2** pour vérifier l'overlap AZ/AZEC.  
Si overlap = 0 → le dedup n'a aucun effet → cherchez ailleurs!
