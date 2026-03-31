
## Cellule 1 — Imports et chemins

```python
import pandas as pd
import numpy as np
import json
from pathlib import Path
from difflib import SequenceMatcher

BASE_DIR = Path("..").resolve()
REFERENCE_DIR = BASE_DIR / "data" / "references"
INPUT_DIR = BASE_DIR / "data" / "inputs"
```

---

## Cellule 2 — Fonctions utilitaires

```python
def detect_separator(file_path, encoding="LATIN9", candidates=(";", ",", "|")):
    """
    Détecte le séparateur à partir de la première ligne du fichier.
    """
    with open(file_path, "r", encoding=encoding) as f:
        first_line = f.readline()

    return max(candidates, key=first_line.count)


def normalize_contract(value):
    """
    Normalise un numéro de contrat sans perdre les zéros à gauche.
    - conserve les zéros à gauche
    - supprime les espaces
    - retire le suffixe '.0' si présent
    - retourne pd.NA si vide
    """
    if pd.isna(value):
        return pd.NA

    value = str(value).strip()

    if value.endswith(".0"):
        value = value[:-2]

    value = value.strip()

    return value if value != "" else pd.NA
```

---

## Cellule 3 — Chargement des données

Adapte juste le nom du fichier Proxima si besoin.

```python
# ------------------------------------------------------------------------------
# Chargement Proxima
# ------------------------------------------------------------------------------
proxima_file = INPUT_DIR / "proxima.csv"

sep_proxima = detect_separator(proxima_file, encoding="LATIN9")
print(f"Séparateur Proxima détecté : '{sep_proxima}'")

proxima_columns = [
    "ID_SITE", "NU_CNT", "NOM_SITE", "NOM_CLI",
    "STOCK", "DT_EFF_RESIL_CNT",
    "PAYS_SITE", "CP_SITE", "VILLE_SITE",
    "NUM_RUE_SITE", "RUE_SITE", "NUM_VOIE_SITE", "LIEU_DIT_SITE", "ADRESSE_SITE",
    "COORD_X_SITE", "COORD_Y_SITE", "QUALITE_GEOCODAGE",
    "CAPITAUX_MURS_BATIMENTS", "CAPITAUX_CONTENU", "CAPITAUX_MATERIEL", "CAPITAUX_MARCHANDISES",
    "CAPITAUX_TT", "CAPITAUX_ASS_MH_HLL_CARAV", "CAPITAUX_DEPENDANCE",
    "CAPITAUX_COMPL", "CAPITAUX_DD",
    "SURFACE", "SURFACE_DPDCE"
]

df_proxima = pd.read_csv(
    proxima_file,
    sep=sep_proxima,
    usecols=proxima_columns
    encoding="LATIN9",
    low_memory=False
)

print(f"✅ Proxima : {len(df_proxima)} lignes, {len(df_proxima.columns)} colonnes")


# ------------------------------------------------------------------------------
# Chargement PTF
# On ne lit que les colonnes utiles au filtre des contrats actifs
# ------------------------------------------------------------------------------
ptf_file = INPUT_DIR / "ptf202602.csv"

sep_ptf = detect_separator(ptf_file, encoding="LATIN9")
print(f"Séparateur PTF détecté : '{sep_ptf}'")

ptf_columns = ["NOPOL", "PTGST", "CDSITP"]

df_ptf = pd.read_csv(
    ptf_file,
    sep=sep_ptf,
    encoding="LATIN9",
    usecols=ptf_columns,
    dtype={"NOPOL": "string"},
    low_memory=False
)

print(f"✅ PTF : {len(df_ptf)} lignes, {len(df_ptf.columns)} colonnes")
display(df_ptf.head())
```

---

## Cellule 4 — Préparation, filtre actifs, jointure et premières stats

```python
# ------------------------------------------------------------------------------
# 1) Normalisation des clés de jointure
# ------------------------------------------------------------------------------
df_proxima = df_proxima.copy()
df_ptf = df_ptf.copy()

df_proxima["NU_CNT"] = df_proxima["NU_CNT"].astype("string").apply(normalize_contract)
df_ptf["NOPOL"] = df_ptf["NOPOL"].apply(normalize_contract)

# Conversion du statut contrat en numérique
df_ptf["CDSITP"] = pd.to_numeric(df_ptf["CDSITP"], errors="coerce")


# ------------------------------------------------------------------------------
# 2) Filtre des contrats actifs dans le datamart
# Règle métier : CDSITP == 1
# ------------------------------------------------------------------------------
df_ptf_actifs = df_ptf.loc[
    df_ptf["CDSITP"] == 1,
    ["NOPOL", "PTGST", "CDSITP"]
].copy()

nb_ptf_actifs_avant_dedup = len(df_ptf_actifs)

df_ptf_actifs = (
    df_ptf_actifs
    .dropna(subset=["NOPOL"])
    .drop_duplicates(subset=["NOPOL"])
    .reset_index(drop=True)
)

nb_ptf_actifs_apres_dedup = len(df_ptf_actifs)


# ------------------------------------------------------------------------------
# 3) Jointure avec Proxima
# On garde uniquement les lignes Proxima dont le contrat est actif
# ------------------------------------------------------------------------------
df_proxima_actifs = df_proxima.merge(
    df_ptf_actifs,
    left_on="NU_CNT",
    right_on="NOPOL",
    how="inner"
)

# NOPOL est redondant avec NU_CNT après jointure
df_proxima_actifs = df_proxima_actifs.drop(columns=["NOPOL"]).copy()


# ------------------------------------------------------------------------------
# 4) Premières statistiques après jointure
# ------------------------------------------------------------------------------
nb_lignes_proxima_total = len(df_proxima)
nb_lignes_proxima_actifs = len(df_proxima_actifs)
nb_lignes_proxima_non_retenues = nb_lignes_proxima_total - nb_lignes_proxima_actifs

nb_contrats_proxima = df_proxima["NU_CNT"].nunique(dropna=True)
nb_contrats_ptf_actifs = df_ptf_actifs["NOPOL"].nunique(dropna=True)
nb_contrats_proxima_actifs = df_proxima_actifs["NU_CNT"].nunique(dropna=True)

taux_lignes_conservees = (
    nb_lignes_proxima_actifs / nb_lignes_proxima_total * 100
    if nb_lignes_proxima_total > 0 else 0
)

print("====================================================")
print("RÉSUMÉ CHARGEMENT / FILTRE CONTRATS ACTIFS / JOINTURE")
print("====================================================")
print(f"Proxima - lignes totales                    : {nb_lignes_proxima_total:,}")
print(f"PTF - contrats actifs avant déduplication   : {nb_ptf_actifs_avant_dedup:,}")
print(f"PTF - contrats actifs après déduplication   : {nb_ptf_actifs_apres_dedup:,}")
print()
print(f"Proxima - lignes retenues après jointure    : {nb_lignes_proxima_actifs:,}")
print(f"Proxima - lignes non retenues               : {nb_lignes_proxima_non_retenues:,}")
print(f"Taux de lignes conservées                   : {taux_lignes_conservees:.2f}%")
print()
print(f"Proxima - contrats distincts                : {nb_contrats_proxima:,}")
print(f"PTF - contrats actifs distincts             : {nb_contrats_ptf_actifs:,}")
print(f"Proxima - contrats actifs matchés           : {nb_contrats_proxima_actifs:,}")
print()
print("Répartition CDSITP après jointure :")
print(df_proxima_actifs["CDSITP"].value_counts(dropna=False))

display(df_proxima_actifs.head())
```

---

Après la jointure et le filtre des contrats actifs :

1. vérifier `PAYS_SITE`
2. si le pays est vide / nul / blanc, le remplacer par **"FRANCE"**
3. pour l’instant, **ne traiter que la France**
4. isoler les lignes hors périmètre

# Code

```python
import pandas as pd

# Copie de travail
df_proxima_actifs = df_proxima_actifs.copy()

# ------------------------------------------------------------------------------
# 1) Normaliser la colonne pays
# ------------------------------------------------------------------------------
df_proxima_actifs["PAYS_SITE"] = (
    df_proxima_actifs["PAYS_SITE"]
    .astype("string")
    .str.strip()
)

# Remplacer les chaînes vides par NA
df_proxima_actifs["PAYS_SITE"] = df_proxima_actifs["PAYS_SITE"].replace("", pd.NA)

# Remplacer les pays non renseignés par FRANCE
df_proxima_actifs["PAYS_SITE"] = df_proxima_actifs["PAYS_SITE"].fillna("FRANCE")

# Harmoniser en majuscules
df_proxima_actifs["PAYS_SITE"] = df_proxima_actifs["PAYS_SITE"].str.upper()

# ------------------------------------------------------------------------------
# 2) Garder uniquement la France pour les traitements suivants
# ------------------------------------------------------------------------------
df_proxima_actifs_france = df_proxima_actifs[
    df_proxima_actifs["PAYS_SITE"] == "FRANCE"
].copy()

# Hors périmètre actuel
df_proxima_actifs_hors_france = df_proxima_actifs[
    df_proxima_actifs["PAYS_SITE"] != "FRANCE"
].copy()

# ------------------------------------------------------------------------------
# 3) Contrôles simples
# ------------------------------------------------------------------------------
nb_actifs_total = len(df_proxima_actifs)
nb_france = len(df_proxima_actifs_france)
nb_hors_france = len(df_proxima_actifs_hors_france)

print(f"Sites actifs total : {nb_actifs_total}")
print(f"Sites actifs France : {nb_france}")
print(f"Sites actifs hors France : {nb_hors_france}")

print("\nRépartition PAYS_SITE après normalisation :")
print(df_proxima_actifs["PAYS_SITE"].value_counts(dropna=False).head(20))
```