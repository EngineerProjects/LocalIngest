"""
Orchestrateur de localisation — Logique cascade GPS → Adresse.

Appelle les modules spécialisés :
- prerequisites.py → P-01, P-07 (toujours)
- gps.py → G-01 à G-07 (si GPS existe)
- address.py → A-01 à A-05 (si GPS absent ou GPS avec anomalies)
- L-01 → Site non localisable (ni GPS ni adresse)
"""

import pandas as pd
from tqdm import tqdm

from src.config import Config, Severity
from src.loader import ReferenceData
from src.models import Anomaly, AnomalyCollector
from src.rules.address import validate_address
from src.rules.gps import validate_gps
from src.rules.prerequisites import check_prerequisites
from src.utils import format_number, is_not_empty


def check_localisation(
    df: pd.DataFrame,
    ref_data: ReferenceData,
    collector: AnomalyCollector,
) -> None:
    """
    Vérifie la localisabilité de chaque site avec la logique cascade :
      GPS existe → valider GPS → si anomalies, vérifier aussi l'adresse
      GPS absent → valider l'adresse → si KO, site non localisable
    """
    print("\n" + "=" * 60)
    print("   VÉRIFICATION DE LOCALISATION (cascade GPS → adresse)")
    print("=" * 60)

    count_gps_ok = 0
    count_gps_anomaly = 0
    count_address_ok = 0
    count_non_localisable = 0
    total_anomalies_before = collector.count()

    for idx, row in tqdm(df.iterrows(), total=len(df), desc="Localisation"):

        site_id = row.get(Config.COL_SITE_ID)
        contract_id = row.get(Config.COL_CONTRACT_ID)

        # --- Toujours : prérequis ---
        check_prerequisites(site_id, contract_id, row, collector)

        # --- Déterminer le chemin ---
        has_gps = (
            is_not_empty(row.get(Config.COL_LONGITUDE))
            or is_not_empty(row.get(Config.COL_LATITUDE))
        )

        if has_gps:
            # === CHEMIN GPS ===
            gps_anomalies = validate_gps(site_id, contract_id, row, ref_data, collector)

            if gps_anomalies == 0:
                count_gps_ok += 1
            else:
                count_gps_anomaly += 1
                # GPS a des anomalies → vérifier aussi l'adresse en complément
                validate_address(site_id, contract_id, row, ref_data, collector)

        else:
            # === CHEMIN ADRESSE (fallback, GPS absent) ===
            addr_anomalies = validate_address(site_id, contract_id, row, ref_data, collector)

            if addr_anomalies == 0:
                count_address_ok += 1
            else:
                count_non_localisable += 1
                collector.add(Anomaly(
                    site_id=site_id,
                    contract_id=contract_id,
                    code="L-01",
                    severity=Severity.GRAVE,
                    description="Site non localisable : pas de GPS et adresse incomplète/invalide.",
                    fields_concerned=[
                        Config.COL_LONGITUDE, Config.COL_LATITUDE,
                        Config.COL_POSTAL_CODE, Config.COL_CITY,
                    ],
                ))

    total_new = collector.count() - total_anomalies_before

    print(f"\n📊 Résultats de localisation :")
    print(f"   Sites analysés         : {format_number(len(df))}")
    print(f"   ✅ GPS valide           : {format_number(count_gps_ok)}")
    print(f"   ⚠️  GPS avec anomalies  : {format_number(count_gps_anomaly)}")
    print(f"   ✅ Adresse valide       : {format_number(count_address_ok)}")
    print(f"   ❌ Non localisable      : {format_number(count_non_localisable)}")
    print(f"   🔍 Anomalies détectées  : {format_number(total_new)}")
    print("\n" + "=" * 60)
