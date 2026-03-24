"""
Verdict de localisation — Bloc E : orchestrateur cascade GPS → Adresse.

Logique pour chaque site actif :
    1. Si pays non supporté → SKIP (ignoré, pas d'anomalie)
    2. Si pays manquant → STOP, L-01
    3. Prérequis (P-07, P-01) — toujours
    4. Si GPS présent → valider GPS
         GPS valide (0 anomalie) → LOCALISÉ ✅ (mode GPS)
         GPS invalide             → basculer sur Adresse
    5. Si GPS absent ou invalide → valider Adresse
         Adresse valide (0 anomalie) → LOCALISÉ ✅ (mode ADRESSE)
         Adresse invalide            → L-01 GRAVE (non localisable)

Les colonnes enrichies ajoutées au DataFrame :
    _IS_LOCALIZED, _LOCALIZATION_MODE, _HAS_ANOMALY,
    _ANOMALY_COUNT, _ANOMALY_CODES, _WORST_SEVERITY, _SUPPORT_LEVEL
"""

import sys

import pandas as pd
from tqdm import tqdm

from src.config import Config, Severity, SupportLevel
from src.loaders.reference_loader import ReferenceLoader
from src.models import Anomaly, AnomalyCollector
from src.utils import format_number, is_not_empty, safe_str
from src.validators.address_validator import validate_address
from src.validators.gps_validator import validate_gps
from src.validators.prerequisites import check_prerequisites


# Ordre de priorité des gravités pour calcul WORST_SEVERITY
_SEVERITY_ORDER = {Severity.GRAVE: 3, Severity.LEGERE: 2, Severity.INFO: 1}


def check_localisation(
    df: pd.DataFrame,
    ref_loader: ReferenceLoader,
    collector: AnomalyCollector,
) -> pd.DataFrame:
    """
    Exécute le contrôle complet de localisation sur tous les sites actifs.
    Les sites dont le pays est NON_SUPPORTÉ sont ignorés (pas d'anomalie générée).

    Returns:
        DataFrame enrichi avec les colonnes _IS_LOCALIZED, _LOCALIZATION_MODE, etc.
    """
    print("\n" + "=" * 60)
    print("   ÉTAPE 3 — VALIDATION DE LOCALISATION")
    print("=" * 60)
    sys.stdout.flush()

    results = []  # list de dicts de résultats par site

    count_gps_ok = 0
    count_gps_anomaly = 0
    count_address_ok = 0
    count_non_localisable = 0
    count_non_supporte = 0  # sites ignorés car pays non supporté

    for idx, row in tqdm(df.iterrows(), total=len(df), desc="Localisation", file=sys.stdout):
        site_id = row.get(Config.COL_SITE_ID)
        contract_id = row.get(Config.COL_CONTRACT_ID)
        anomalies_start = collector.count()

        # Récupérer le pays et vérifier le support
        country_raw = row.get(Config.COL_COUNTRY)
        country = safe_str(country_raw).upper() if is_not_empty(country_raw) else ""
        support_level = ref_loader.get_support_level(country) if country else SupportLevel.NON_SUPPORTE

        # === Sites ignorés : pays non supporté (pas d'anomalie générée) ===
        if support_level == SupportLevel.NON_SUPPORTE:
            count_non_supporte += 1
            results.append({
                "_IS_LOCALIZED": False,
                "_LOCALIZATION_MODE": "IGNORÉ",
                "_HAS_ANOMALY": False,
                "_ANOMALY_COUNT": 0,
                "_ANOMALY_CODES": "",
                "_WORST_SEVERITY": "",
                "_SUPPORT_LEVEL": SupportLevel.NON_SUPPORTE,
            })
            continue

        # === Blocs A + B : Prérequis ===
        country_ok, support_level = check_prerequisites(
            site_id, contract_id, row, ref_loader, collector
        )

        localized = False
        loc_mode = "AUCUN"

        if not country_ok:
            # Pays manquant → non localisable direct
            _add_l01(site_id, contract_id, collector)
            count_non_localisable += 1
        else:
            # === Bloc C : GPS présent ? ===
            has_gps = (
                is_not_empty(row.get(Config.COL_LONGITUDE))
                or is_not_empty(row.get(Config.COL_LATITUDE))
            )

            if has_gps:
                gps_anomalies = validate_gps(
                    site_id, contract_id, row, ref_loader, support_level, collector
                )

                if gps_anomalies == 0:
                    # GPS valide
                    localized = True
                    loc_mode = "GPS"
                    count_gps_ok += 1
                else:
                    # GPS invalide → Bloc D : Adresse
                    count_gps_anomaly += 1
                    addr_anomalies = validate_address(
                        site_id, contract_id, row, ref_loader, collector
                    )
                    if addr_anomalies == 0:
                        localized = True
                        loc_mode = "ADRESSE"
                        count_address_ok += 1
                    else:
                        _add_l01(site_id, contract_id, collector)
                        count_non_localisable += 1

            else:
                # Pas de GPS → Bloc D : Adresse directement
                addr_anomalies = validate_address(
                    site_id, contract_id, row, ref_loader, collector
                )
                if addr_anomalies == 0:
                    localized = True
                    loc_mode = "ADRESSE"
                    count_address_ok += 1
                else:
                    _add_l01(site_id, contract_id, collector)
                    count_non_localisable += 1

        # Calcul des anomalies de ce site
        site_anomalies = collector.anomalies[anomalies_start:]
        anomaly_codes = sorted({a.code for a in site_anomalies})
        worst = _worst_severity(site_anomalies)

        results.append({
            "_IS_LOCALIZED": localized,
            "_LOCALIZATION_MODE": loc_mode,
            "_HAS_ANOMALY": len(site_anomalies) > 0,
            "_ANOMALY_COUNT": len(site_anomalies),
            "_ANOMALY_CODES": ", ".join(anomaly_codes),
            "_WORST_SEVERITY": worst,
            "_SUPPORT_LEVEL": support_level,
        })

    # Ajouter les colonnes enrichies au DataFrame
    enriched_cols = pd.DataFrame(results, index=df.index)
    df_enriched = pd.concat([df, enriched_cols], axis=1)

    # sites effectivement analysés (pays supportés)
    count_analysed = len(df) - count_non_supporte

    # Résumé
    sys.stdout.flush()
    print(f"\n📊 Résultats de localisation :")
    print(f"   Sites traités            : {format_number(count_analysed)}")
    print(f"   ⛔ Pays non supporté      : {format_number(count_non_supporte)} (ignorés)")
    print(f"   ✅ GPS valide            : {format_number(count_gps_ok)}")
    print(f"   ⚠️  GPS avec anomalies  : {format_number(count_gps_anomaly)}")
    print(f"   ✅ Adresse valide        : {format_number(count_address_ok)}")
    print(f"   ❌ Non localisable        : {format_number(count_non_localisable)}")
    print(f"   🔍 Total anomalies        : {format_number(collector.count())}")
    print("\n" + "=" * 60)

    return df_enriched


# =============================================================================
# HELPERS PRIVÉS
# =============================================================================

def _add_l01(site_id, contract_id, collector: AnomalyCollector) -> None:
    """Ajoute l'anomalie L-01 (site non localisable)."""
    collector.add(Anomaly(
        site_id=site_id,
        contract_id=contract_id,
        code="L-01",
        severity=Severity.GRAVE,
        description="Site non localisable : pas de GPS valide et adresse incomplète ou invalide.",
        fields_concerned=[
            Config.COL_LONGITUDE,
            Config.COL_LATITUDE,
            Config.COL_POSTAL_CODE,
            Config.COL_CITY,
        ],
    ))


def _worst_severity(anomalies) -> str:
    """Retourne la gravité la plus élevée parmi une liste d'anomalies."""
    if not anomalies:
        return ""
    return max(anomalies, key=lambda a: _SEVERITY_ORDER.get(a.severity, 0)).severity