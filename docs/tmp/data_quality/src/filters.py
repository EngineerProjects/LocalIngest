import pandas as pd

from src.utils import format_number, is_empty, normalize_country


def filter_active_contracts(
    df: pd.DataFrame,
    df_datamart: pd.DataFrame,
) -> pd.DataFrame:
    """
    Filtre les contrats actifs via jointure avec datamart PTF.
    Enrichit avec PTGST (point de gestion) depuis le datamart.
    """
    print("\n" + "=" * 60)
    print("   ÉTAPE 1 — FILTRE CONTRATS ACTIFS")
    print("=" * 60)

    initial_count = len(df)

    if df_datamart.empty:
        print("Datamart vide ou absent, aucun filtre appliqué")
        return df.copy()

    df = df.copy()
    df_datamart = df_datamart.copy()

    df_ptf_actifs = df_datamart[
        df_datamart["CDSITP"].astype(str).str.strip() == "1"
    ].copy()

    nb_avant_dedup = len(df_ptf_actifs)
    df_ptf_actifs = df_ptf_actifs.dropna(subset=["NOPOL"]).drop_duplicates(
        subset=["NOPOL"]
    )
    nb_apres_dedup = len(df_ptf_actifs)

    df_ptf_actifs = df_ptf_actifs[["NOPOL", "PTGST"]].copy()
    df_ptf_actifs = df_ptf_actifs.rename(columns={"NOPOL": "NU_CNT"})

    df_enriched = df.merge(df_ptf_actifs, on="NU_CNT", how="inner")

    filtered_count = len(df_enriched)
    removed_count = initial_count - filtered_count

    print(f"\nRésultats :")
    print(f"   Lignes initiales (Proxima)     : {format_number(initial_count)}")
    print(f"   PTF actifs avant déduplication : {format_number(nb_avant_dedup)}")
    print(f"   PTF actifs après déduplication  : {format_number(nb_apres_dedup)}")
    print(f"   Contrats actifs après jointure  : {format_number(filtered_count)}")
    print(f"   Contrats inactifs               : {format_number(removed_count)}")
    print(
        f"   Taux de rétention               : {(filtered_count / initial_count) * 100:.2f}%"
    )

    print("\n" + "=" * 60)
    return df_enriched


def handle_country_filter(
    df: pd.DataFrame,
    supported_countries: list = None,
) -> pd.DataFrame:
    """
    Filtre par pays:
    - Pays vide → défaut France (si supporté)
    - Pays non supporté → exclure

    Args:
        df: DataFrame à filtrer
        supported_countries: Liste des pays avec références complètes
    """
    print("\n" + "=" * 60)
    print("   ÉTAPE 2 — FILTRE PAYS")
    print("=" * 60)

    if supported_countries is None:
        supported_countries = ["FRANCE"]

    supported = [c.upper() for c in supported_countries]

    print(f"   Pays supportés : {', '.join(supported)}")

    initial_count = len(df)

    def classify_country(row):
        country = normalize_country(row.get("PAYS_SITE"))

        if is_empty(country):
            if "FRANCE" in supported:
                return "DEFAULT_FRANCE"
            return "NON_SUPPORTÉ"

        if country in supported:
            return country

        return "NON_SUPPORTÉ"

    df["_COUNTRY_TYPE"] = df.apply(classify_country, axis=1)

    if "DEFAULT_FRANCE" in df["_COUNTRY_TYPE"].values:
        df.loc[df["_COUNTRY_TYPE"] == "DEFAULT_FRANCE", "PAYS_SITE"] = "FRANCE"

    allowed_types = supported + ["DEFAULT_FRANCE"]
    df_filtered = df[df["_COUNTRY_TYPE"].isin(allowed_types)].copy()
    df_filtered = df_filtered.drop(columns=["_COUNTRY_TYPE"])

    not_supported_count = (df["_COUNTRY_TYPE"] == "NON_SUPPORTÉ").sum()
    default_count = (df["_COUNTRY_TYPE"] == "DEFAULT_FRANCE").sum()

    print(f"\nRésultats :")
    print(f"   Lignes avant filtre  : {format_number(initial_count)}")
    print(f"   Pays supportés        : {format_number(len(df_filtered))}")
    print(f"   Pays non supportés   : {format_number(not_supported_count)}")
    if default_count > 0:
        print(f"   Pays vide → France   : {format_number(default_count)}")

    print("\n" + "=" * 60)
    return df_filtered
