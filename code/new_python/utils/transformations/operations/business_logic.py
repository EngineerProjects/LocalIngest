# -*- coding: utf-8 -*-
"""
Logique métier pour le Pipeline de Données Construction.

Ce module fournit :
- L'extraction des capitaux à partir des champs étiquetés (côté AZ, LBCAPI*/MTCAPI*)
- Les indicateurs de mouvement :
    * AZ :    calculate_az_movements (AFN, RES, RPT, RPC, NBPTF)
    * AZEC :  calculate_azec_movements (NBAFN, NBRES, NBPTF)
- Le calcul des expositions (YTD, GLI) avec un mappage de colonnes (AZ / AZEC)
- Le calcul de suspension (AZEC uniquement)

Notes de conception
-------------------
- Les logiques de mouvement AZ et AZEC sont différentes et DOIVENT rester des fonctions séparées.
- Les littéraux de date sont toujours convertis en DateType (vs dates numériques en SAS).
- Pas de dépendance à eval() en dehors des transformations contrôlées et explicites.
- Toutes les sorties utilisent des noms de colonnes en minuscules pour assurer la cohérence.
"""

from typing import Dict, Any, List, Optional
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql.functions import (
    col, when, lit, coalesce, upper, greatest, least, to_date,
    year as year_func, month as month_func, datediff, date_add, date_sub
)

# ---------------------------------------------------------------------------
# Utilitaires
# ---------------------------------------------------------------------------

def _to_date_lit(d: str):
    """Retourne un littéral de date typé (yyyy-MM-dd) comme une Colonne PySpark."""
    return to_date(lit(d), 'yyyy-MM-dd')

def _get_date_columns_from_context(dates: Dict[str, str]) -> Dict[str, Any]:
    """
    Normalise les clés de date provenant de helpers.compute_date_ranges() (minuscules).
    Retourne des colonnes de date Spark typées correspondant au nommage métier.
    """
    return {
        "DTFIN":    _to_date_lit(dates["dtfin"]),
        "DTDEB_AN": _to_date_lit(dates["dtdeb_an"]),
        "DTFINMN":  _to_date_lit(dates["dtfinmn"]),
        "DTFINMN1": _to_date_lit(dates["dtfinmm1"]),
    }

# ---------------------------------------------------------------------------
# Capitaux (AZ – scan LBCAPI/MTCAPI via config)
# ---------------------------------------------------------------------------

def extract_capitals(df: DataFrame, config: Dict[str, Dict]) -> DataFrame:
    """
    Extrait les montants de capital en scannant les champs étiquetés (LBCAPIk / MTCAPIk).

    Le format de configuration (par colonne cible) :
    {
      "perte_exp": {
        "keywords": ["PERTE EXPLOITATION", "PERTES D'EXPLOITATION", ...],
        "exclude_keywords": [],
        "label_prefix": "lbcapi",
        "amount_prefix": "mtcapi",
        "num_indices": 14
      },
      ...
    }

    Comportement métier ("la dernière écriture l'emporte") :
    - Itérer k = 1..14. Si une étiquette correspond aux mots-clés (insensible à la casse)
      et ne correspond à aucun mot-clé exclu, assigner MTCAPIk à la cible.
    - Le dernier k correspondant écrase les résultats précédents.
    """
    for target_col, extraction_config in config.items():
        keywords = extraction_config['keywords']
        exclude_keywords = extraction_config.get('exclude_keywords', [])
        label_prefix = extraction_config['label_prefix'].lower()
        amount_prefix = extraction_config['amount_prefix'].lower()
        num_indices = int(extraction_config['num_indices'])

        # Défaut (initialisé à 0)
        result_expr = lit(0.0).cast(DoubleType())

        for i in range(1, num_indices + 1):
            label_col = f"{label_prefix}{i}"
            amount_col = f"{amount_prefix}{i}"
            if label_col not in df.columns or amount_col not in df.columns:
                continue

            kw_exprs = [upper(col(label_col)).contains(kw.upper()) for kw in keywords]
            if not kw_exprs:
                continue
            match_expr = reduce(lambda a, b: a | b, kw_exprs)

            if exclude_keywords:
                ex_exprs = [upper(col(label_col)).contains(ek.upper()) for ek in exclude_keywords]
                match_expr = match_expr & (~reduce(lambda a, b: a | b, ex_exprs))

            # "la dernière écriture l'emporte"
            result_expr = when(match_expr, coalesce(col(amount_col), lit(0.0))).otherwise(result_expr)

        df = df.withColumn(target_col.lower(), result_expr)

    return df


# ---------------------------------------------------------------------------
# Mouvements — AZ
# ---------------------------------------------------------------------------

def calculate_az_movements(
    df: DataFrame,
    dates: Dict[str, str],
    annee: int,
    column_mapping: Optional[Dict[str, str]] = None
) -> DataFrame:
    """
    Calcule les indicateurs de mouvement AZ (AFN, RES, RPT, RPC, NBPTF).

    Règles métier (Étape 5) :
      - NBPTF : CSSSEG != '5' ET CDNATP dans ('R','O')
                ET ( (cdsitp='1' ET dtcrepol <= DTFIN) OU (cdsitp='3' ET dtresilp > DTFIN) )
      - AFN :
          (DTDEB_AN <= dteffan <= DTFIN ET DTDEB_AN <= dttraan <= DTFIN)
        OU (DTDEB_AN <= dtcrepol <= DTFIN)
        OU (year(dteffan) < year(DTFIN) ET DTDEB_AN <= dttraan <= DTFIN)
      - RES (hors chantiers) :
          CDNATP != 'C' ET cdsitp='3' ET
          ( (DTDEB_AN <= dtresilp <= DTFIN)
            OU (DTDEB_AN <= dttraar <= DTFIN ET dtresilp <= DTFIN) )
      - RPC (remplacé) :
          NBRES=1 + RP sur n'importe quel cdtypli* dans l'année(DTFIN)
      - RPT (remplaçant) :
          NBAFN=1 + RE sur n'importe quel cdtypli* dans l'année(DTFIN)

    Préconditions :
      - 'primeto' existe. Sinon, sera calculé si 'mtprprto' et 'tx' sont présents.
    """
    # Valeurs par défaut des colonnes (Schéma AZ)
    creation_date   = "dtcrepol"
    effective_date  = "dteffan"
    termination_date= "dtresilp"
    transfer_start  = "dttraan"
    transfer_end    = "dttraar"
    type_col1, type_col2, type_col3 = "cdtypli1", "cdtypli2", "cdtypli3"
    type_date1, type_date2, type_date3 = "dttypli1", "dttypli2", "dttypli3"

    # Appliquer les surcharges si fournies
    if column_mapping:
        creation_date   = column_mapping.get('creation_date',   creation_date)
        effective_date  = column_mapping.get('effective_date',  effective_date)
        termination_date= column_mapping.get('termination_date',termination_date)
        transfer_start  = column_mapping.get('transfer_start',  transfer_start)
        transfer_end    = column_mapping.get('transfer_end',    transfer_end)
        type_col1       = column_mapping.get('type_col1',       type_col1)
        type_col2       = column_mapping.get('type_col2',       type_col2)
        type_col3       = column_mapping.get('type_col3',       type_col3)
        type_date1      = column_mapping.get('type_date1',      type_date1)
        type_date2      = column_mapping.get('type_date2',      type_date2)
        type_date3      = column_mapping.get('type_date3',      type_date3)

    # S'assurer que les colonnes existent en minuscules
    for c in [creation_date, effective_date, termination_date, transfer_start, transfer_end,
              type_col1, type_col2, type_col3, type_date1, type_date2, type_date3]:
        # Créer les colonnes manquantes comme NULL pour éviter les plantages (défensif)
        if c and c not in df.columns:
            df = df.withColumn(c, lit(None))

    # Dates
    d = _get_date_columns_from_context(dates)
    dtfin, dtdeb_an = d["DTFIN"], d["DTDEB_AN"]

    # S'assurer de primeto
    if "primeto" not in df.columns:
        if "mtprprto" in df.columns and "tx" in df.columns:
            df = df.withColumn("primeto", col("mtprprto") * (lit(1.0) - (col("tx") / 100.0)))
        else:
            df = df.withColumn("primeto", lit(0.0))

    # Initialiser les sorties si manquantes
    for c in ['nbafn','nbres','nbrpt','nbrpc','nbptf','primes_afn','primes_res','primes_rpt','primes_rpc','primes_ptf']:
        if c not in df.columns:
            df = df.withColumn(c, lit(0))

    # --- NBPTF ---
    nbptf_cond = (
        ~col('cssseg').isin(['5']) &  # Null-safe explicite (exclut les NULLs)
        (col('cdnatp').isin('R', 'O')) &
        (
            ((col('cdsitp') == '1') & (col(creation_date) <= dtfin)) |
            ((col('cdsitp') == '3') & (col(termination_date) >  dtfin))
        )
    )
    df = df.withColumn("nbptf", when(nbptf_cond, lit(1)).otherwise(lit(0))) \
           .withColumn("primes_ptf", when(col("nbptf") == 1, col("primeto")).otherwise(lit(0)))

    # --- AFN ---
    afn_cond = (
        (
            (col(effective_date).isNotNull()) &
            (col(transfer_start).isNotNull()) &
            (col(effective_date) >= dtdeb_an) & (col(effective_date) <= dtfin) &
            (col(transfer_start) >= dtdeb_an) & (col(transfer_start) <= dtfin)
        ) |
        (
            (col(creation_date).isNotNull()) &
            (col(creation_date) >= dtdeb_an) & (col(creation_date) <= dtfin)
        ) |
        (
            (col(effective_date).isNotNull()) &
            (year_func(col(effective_date)) < year_func(dtfin)) &
            (col(transfer_start).isNotNull()) &
            (col(transfer_start) >= dtdeb_an) & (col(transfer_start) <= dtfin)
        )
    )
    df = df.withColumn("nbafn", when(afn_cond, lit(1)).otherwise(lit(0))) \
           .withColumn("primes_afn", when(col("nbafn") == 1, col("primeto")).otherwise(lit(0)))

    # --- RES (hors chantiers) ---
    res_cond = (
        ~col("cdnatp").isin(['C']) &  # Null-safe explicite (exclut les NULLs)
        (col("cdsitp") == "3") &
        (
            ((col(termination_date).isNotNull()) &
             (col(termination_date) >= dtdeb_an) & (col(termination_date) <= dtfin)) |
            ((col(transfer_end).isNotNull()) &
             (col(transfer_end) >= dtdeb_an) & (col(transfer_end) <= dtfin) &
             (col(termination_date).isNotNull()) & (col(termination_date) <= dtfin))
        )
    )
    df = df.withColumn("nbres", when(res_cond, lit(1)).otherwise(lit(0))) \
           .withColumn("primes_res", when(col("nbres") == 1, col("primeto")).otherwise(lit(0)))

    # --- RPC (remplacé) ---
    rpc_cond = (col("nbres") == 1) & (
        ((col(type_col1) == "RP") & (year_func(col(type_date1)) == year_func(dtfin))) |
        ((col(type_col2) == "RP") & (year_func(col(type_date2)) == year_func(dtfin))) |
        ((col(type_col3) == "RP") & (year_func(col(type_date3)) == year_func(dtfin)))
    )
    df = df.withColumn("nbrpc", when(rpc_cond, lit(1)).otherwise(lit(0))) \
           .withColumn("primes_rpc", when(col("nbrpc") == 1, col("primeto")).otherwise(lit(0))) \
           .withColumn("nbres", when(col("nbrpc") == 1, lit(0)).otherwise(col("nbres"))) \
           .withColumn("primes_res", when(col("nbrpc") == 1, lit(0)).otherwise(col("primes_res")))

    # --- RPT (remplaçant) ---
    rpt_cond = (col("nbafn") == 1) & (
        ((col(type_col1) == "RE") & (year_func(col(type_date1)) == year_func(dtfin))) |
        ((col(type_col2) == "RE") & (year_func(col(type_date2)) == year_func(dtfin))) |
        ((col(type_col3) == "RE") & (year_func(col(type_date3)) == year_func(dtfin)))
    )
    df = df.withColumn("nbrpt", when(rpt_cond, lit(1)).otherwise(lit(0))) \
           .withColumn("primes_rpt", when(col("nbrpt") == 1, col("primeto")).otherwise(lit(0))) \
           .withColumn("nbafn", when(col("nbrpt") == 1, lit(0)).otherwise(col("nbafn"))) \
           .withColumn("primes_afn", when(col("nbrpt") == 1, lit(0)).otherwise(col("primes_afn")))

    return df




# ---------------------------------------------------------------------------
# Mouvements — AZEC
# ---------------------------------------------------------------------------

def calculate_azec_movements(
    df: DataFrame,
    dates: Dict[str, str],
    annee: int,
    mois: int
) -> DataFrame:
    """
    Calcule les indicateurs de mouvement spécifiques AZEC (NBAFN, NBRES, NBPTF).

    Différences clés vs AZ :
      - Comportement spécifique au produit (liste de produits)
      - Champs de date : effetpol/datafn/datfin/datresil
      - Utilise NBPTF_NON_MIGRES_AZEC
    """
    from config.variables import AZEC_PRODUIT_LIST

    d = _get_date_columns_from_context(dates)
    dtdeb_an, dtfinmn = d["DTDEB_AN"], d["DTFINMN"]

    # Initialiser
    for c in ["nbafn", "nbres", "nbptf"]:
        if c not in df.columns:
            df = df.withColumn(c, lit(0))

    # Conditions de base
    base_afn = (
        (col("etatpol") == "R") &
        (~col("produit").isin("CNR", "DO0")) &
        (col("nbptf_non_migres_azec") == 1)
    )
    base_res = (
        (col("etatpol") == "R") &
        (~col("produit").isin("CNR", "DO0")) &
        (col("nbptf_non_migres_azec") == 1)
    )

    # AFN — produits spéciaux vs autres
    afn_in_list = (
        col("produit").isin(AZEC_PRODUIT_LIST) &
        (month_func(col("datafn")) <= lit(mois)) &
        (year_func(col("datafn")) == lit(annee))
    )
    afn_not_in_list = (
        ~col("produit").isin(AZEC_PRODUIT_LIST) &
        (
            (
                col("effetpol").isNotNull() & col("datafn").isNotNull() &
                (col("effetpol") >= dtdeb_an) & (col("effetpol") <= dtfinmn) &
                (col("datafn")   <= dtfinmn)
            ) |
            (
                col("effetpol").isNotNull() & col("datafn").isNotNull() &
                (col("effetpol") <  dtdeb_an) &
                (col("datafn")   >= dtdeb_an) & (col("datafn") <= dtfinmn)
            )
        )
    )
    df = df.withColumn("nbafn", when(base_afn & (afn_in_list | afn_not_in_list), lit(1)).otherwise(lit(0)))

    # RES — produits spéciaux vs autres
    res_in_list = (
        col("produit").isin(AZEC_PRODUIT_LIST) &
        (month_func(col("datresil")) <= lit(mois)) &
        (year_func(col("datresil")) == lit(annee))
    )
    res_not_in_list = (
        ~col("produit").isin(AZEC_PRODUIT_LIST) &
        (
            (
                col("datfin").isNotNull() & col("datresil").isNotNull() &
                (col("datfin")    >= dtdeb_an) & (col("datfin")    <= dtfinmn) &
                (col("datresil")  <= dtfinmn)
            ) |
            (
                col("datfin").isNotNull() & col("datresil").isNotNull() &
                (col("datfin")    <= dtfinmn) &
                (col("datresil")  >= dtdeb_an) & (col("datresil") <= dtfinmn)
            )
        )
    )
    df = df.withColumn("nbres", when(base_res & (res_in_list | res_not_in_list), lit(1)).otherwise(lit(0)))

    # NBPTF — actif à fin de mois, hors DO0/TRC/CTR/CNR
    nbptf_cond = (
        (col("nbptf_non_migres_azec") == 1) &
        col("effetpol").isNotNull() & (col("effetpol") <= dtfinmn) &
        col("datafn").isNotNull()   & (col("datafn")   <= dtfinmn) &
        (
            col("datfin").isNull() |
            (col("datfin")   > dtfinmn) |
            (col("datresil") > dtfinmn)
        ) &
        (
            (col("etatpol") == "E") |
            ((col("etatpol") == "R") & (col("datfin") >= dtfinmn))
        ) &
        (~col("produit").isin("DO0", "TRC", "CTR", "CNR"))
    )
    df = df.withColumn("nbptf", when(nbptf_cond, lit(1)).otherwise(lit(0)))

    return df

# ---------------------------------------------------------------------------
# Expositions (AZ / AZEC via mapping)
# ---------------------------------------------------------------------------

def calculate_exposures(
    df: DataFrame,
    dates: Dict[str, str],
    annee: int,
    column_mapping: Dict[str, str]
) -> DataFrame:
    """
    Calcule expo_ytd et expo_gli en utilisant des fenêtres inclusives.

    Le mapping fournit :
      - creation_date : dtcrepol (AZ) ou effetpol (AZEC)
      - termination_date : dtresilp (AZ) ou datfin (AZEC)

    YTD :
      expo_ytd = max(0, (min(term, DTFIN) - max(crea, DTDEB_AN) + 1)) / (Dec31(annee) - DTDEB_AN + 1)
    GLI (mois courant) :
      expo_gli = max(0, (min(term, DTFIN) - max(crea, DTFINMN1+1) + 1)) / (DTFIN - DTFINMN1)

    Le périmètre de sélection correspond aux conditions WHERE utilisées pour les portées d'exposition.
    """
    creation_date = column_mapping['creation_date']
    termination_date = column_mapping['termination_date']

    # Dates
    d = _get_date_columns_from_context(dates)
    dtfin, dtdeb_an, dtfinmn, dtfinmn1 = d["DTFIN"], d["DTDEB_AN"], d["DTFINMN"], d["DTFINMN1"]
    dtdeb_mois = date_add(dtfinmn1, 1)

    # Périmètre d'exposition. Cette clause est orientée AZ mais sûre pour AZEC si le mapping correspond.
    expo_where_cond = lit(True)
    if 'cdnatp' in df.columns and 'cdsitp' in df.columns:
        branch1 = (
            (col('cdnatp').isin('R','O')) &
            (
                ((col('cdsitp') == '1') & (col(creation_date) <= dtfinmn)) |
                ((col('cdsitp') == '3') & (col(termination_date) >  dtfinmn))
            )
        )
        branch2 = (
            (col('cdsitp').isin('1','3')) &
            (coalesce(col('cdnatp'), lit('X')) != 'F') &
            (
                ((col(creation_date) <= dtfinmn) &
                 (col(termination_date).isNull() |
                  ((col(termination_date) >= dtfinmn1) & (col(termination_date) < dtfinmn)))) |
                ((col(creation_date) <= dtfinmn) & (col(termination_date) > dtfinmn)) |
                ((col(creation_date) >  dtfinmn1) & (col(creation_date) <= dtfinmn) &
                 (col(termination_date) >= dtfinmn1))
            )
        )
        expo_where_cond = branch1 | branch2

    # Dénominateurs
    year_end = _to_date_lit(f"{annee}-12-31")
    denom_ytd = (datediff(year_end, dtdeb_an) + 1)  # inclusif
    denom_gli = datediff(dtfin, dtfinmn1)           # jours dans le mois courant

    # Numérateurs
    num_ytd = (datediff(least(col(termination_date), dtfin), greatest(col(creation_date), dtdeb_an)) + 1)
    num_gli = (datediff(least(col(termination_date), dtfin), greatest(col(creation_date), dtdeb_mois)) + 1)

    df = df.withColumn(
        "expo_ytd",
        when(
            expo_where_cond &
            (col(creation_date) <= dtfin) &
            (col(termination_date) >= dtdeb_an),
            (when(num_ytd > 0, num_ytd).otherwise(lit(0))) / denom_ytd
        ).otherwise(lit(0.0))
    ).withColumn(
        "dt_deb_expo",
        greatest(col(creation_date), dtdeb_an)
    ).withColumn(
        "dt_fin_expo",
        when(col(termination_date).isNotNull() & (col(termination_date) < dtfin), col(termination_date))
        .otherwise(dtfin)
    ).withColumn(
        "expo_gli",
        when(
            expo_where_cond &
            (col(creation_date) <= dtfin) &
            (col(termination_date) >= dtdeb_mois) &
            (denom_gli > 0),
            (when(num_gli > 0, num_gli).otherwise(lit(0))) / denom_gli
        ).otherwise(lit(0.0))
    )

    return df

def calculate_exposures_azec(
    df: DataFrame,
    dates: Dict[str, str]
) -> DataFrame:
    """
    Calcule les expositions AZEC (YTD, GLI) exactement comme règle métier (pas de périmètre strict).
      - expo_ytd = max(0, (min(datfin, DTFIN) - max(effetpol, DTDEB_AN) + 1)) / (Dec31(annee) - DTDEB_AN + 1)
      - expo_gli = max(0, (min(datfin, DTFIN) - max(effetpol, DTFINMN1 + 1) + 1)) / (DTFIN - DTFINMN1)
      - dt_deb_expo = max(effetpol, DTDEB_AN)
      - dt_fin_expo = min(datfin, DTFIN)
    """
    d = _get_date_columns_from_context(dates)
    dtfin, dtdeb_an, dtfinmn, dtfinmn1 = d["DTFIN"], d["DTDEB_AN"], d["DTFINMN"], d["DTFINMN1"]
    dtdeb_mois = date_add(dtfinmn1, 1)

    # Dénominateurs (inclusif YTD)
    year_end = _to_date_lit(str(int(dates['dtdeb_an'][:4]) + 0) + "-12-31")  # annee = dtdeb_an.year
    denom_ytd = (datediff(year_end, dtdeb_an) + 1)
    denom_gli = datediff(dtfin, dtfinmn1)  # nb jours dans le mois courant

    # Numérateurs (inclusif +1)
    num_ytd = (datediff(least(col("datfin"), dtfin), greatest(col("effetpol"), dtdeb_an)) + 1)
    num_gli = (datediff(least(col("datfin"), dtfin), greatest(col("effetpol"), dtdeb_mois)) + 1)

    df = df.withColumn(
        "expo_ytd",
        when(
            (col("effetpol").isNotNull()) & (col("datfin").isNotNull()),
            (when(num_ytd > 0, num_ytd).otherwise(lit(0))) / denom_ytd
        ).otherwise(lit(0.0))
    ).withColumn(
        "dt_deb_expo",
        greatest(col("effetpol"), dtdeb_an)
    ).withColumn(
        "dt_fin_expo",
        least(col("datfin"), dtfin)
    ).withColumn(
        "expo_gli",
        when(
            (col("effetpol").isNotNull()) & (col("datfin").isNotNull()) & (denom_gli > 0),
            (when(num_gli > 0, num_gli).otherwise(lit(0))) / denom_gli
        ).otherwise(lit(0.0))
    )

    return df

# ---------------------------------------------------------------------------
# Suspension — AZEC
# ---------------------------------------------------------------------------

def calculate_azec_suspension(df: DataFrame, dates: Dict[str, str]) -> DataFrame:
    """
    Calcule nbj_susp_ytd (jours de suspension) pour AZEC.
    CAS
      QUAND (DTDEBN <= datresil <= DTFINMN) OU (DTDEBN <= datfin <= DTFINMN)
        ALORS min(datfin, DTFINMN, datexpir) - max(DTDEBN-1, datresil-1)
      QUAND (datresil <= DTDEBN) ET (datfin >= DTFINMN)
        ALORS (DTFINMN - DTDEBN + 1)
      SINON 0
    FIN
    """
    dtdebn = _to_date_lit(dates['dtdebn'])
    dtfinmn = _to_date_lit(dates['dtfinmn'])

    cond1 = (
        ((col("datresil") >= dtdebn) & (col("datresil") <= dtfinmn)) |
        ((col("datfin")   >= dtdebn) & (col("datfin")   <= dtfinmn))
    )
    susp_days_1 = datediff(
        least(col("datfin"), dtfinmn, col("datexpir")),
        greatest(date_sub(dtdebn, 1), date_sub(col("datresil"), 1))
    )

    cond2 = (
        col("datresil").isNotNull() &
        (col("datresil") <= dtdebn) &
        (col("datfin")   >= dtfinmn)
    )
    susp_days_2 = datediff(dtfinmn, dtdebn) + 1

    return df.withColumn(
        "nbj_susp_ytd",
        when(cond1, susp_days_1).when(cond2, susp_days_2).otherwise(lit(0))
    )