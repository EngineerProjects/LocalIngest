"""
Setup — Préparation des données de référence.

Ce module gère :
1. La vérification des fichiers source (departements.geojson, codes_postaux_france.csv, bbox_pays.json)
2. La génération des fichiers dérivés (bbox_departements.json depuis departements.geojson)
3. Le diagnostic de l'état du projet avant exécution

Doit être exécuté une fois avant le premier run, ou à chaque mise à jour des données source.
"""

import json
from pathlib import Path
from typing import List, Optional, Tuple

from src.config import Config


# =============================================================================
# EXTRACTION DE COORDONNÉES DEPUIS GEOJSON
# =============================================================================

def extract_all_coordinates(geometry: dict) -> List[Tuple[float, float]]:
    """
    Extrait toutes les coordonnées d'une géométrie GeoJSON.
    Gère les types Polygon et MultiPolygon.

    Returns:
        Liste de tuples (lon, lat)
    """
    coords = []
    geom_type = geometry.get("type")
    coordinates = geometry.get("coordinates", [])

    if geom_type == "Polygon":
        for ring in coordinates:
            for point in ring:
                coords.append((point[0], point[1]))

    elif geom_type == "MultiPolygon":
        for polygon in coordinates:
            for ring in polygon:
                for point in ring:
                    coords.append((point[0], point[1]))
    else:
        print(f"⚠️ Type de géométrie non supporté : {geom_type}")

    return coords


def compute_bbox(coordinates: List[Tuple[float, float]]) -> Optional[List[float]]:
    """
    Calcule la bounding box à partir d'une liste de coordonnées.

    Returns:
        [lon_min, lat_min, lon_max, lat_max] ou None si vide
    """
    if not coordinates:
        return None

    lons = [c[0] for c in coordinates]
    lats = [c[1] for c in coordinates]

    return [
        round(min(lons), 4),  # lon_min
        round(min(lats), 4),  # lat_min
        round(max(lons), 4),  # lon_max
        round(max(lats), 4),  # lat_max
    ]


# =============================================================================
# GÉNÉRATION DES BOUNDING BOXES DÉPARTEMENTS
# =============================================================================

def generate_bbox_departements(
    input_path: Path = None,
    output_path: Path = None,
) -> bool:
    """
    Génère bbox_departements.json à partir de departements.geojson.

    Lit le fichier GeoJSON, extrait les contours de chaque département,
    calcule la bounding box, et sauvegarde le résultat au format :
    {
        "01": {"name": "Ain", "bbox": [lon_min, lat_min, lon_max, lat_max]},
        "02": {"name": "Aisne", "bbox": [...]},
        ...
    }

    Returns:
        True si succès, False sinon
    """
    input_path = input_path or Config.GEOJSON_DEPT_FILE
    output_path = output_path or Config.BBOX_DEPT_FILE

    print(f"📂 Lecture du fichier : {input_path.name}")

    if not input_path.exists():
        print(f"❌ Fichier source non trouvé : {input_path}")
        return False

    # Charger le GeoJSON
    with open(input_path, "r", encoding="utf-8") as f:
        geojson_data = json.load(f)

    features = geojson_data.get("features", [])
    print(f"   {len(features)} départements trouvés")

    # Générer les bounding boxes
    bbox_dict = {}
    errors = []

    for feature in features:
        properties = feature.get("properties", {})
        geometry = feature.get("geometry", {})

        code = properties.get("code")
        nom = properties.get("nom")

        if not code:
            errors.append(f"Feature sans code : {properties}")
            continue

        coordinates = extract_all_coordinates(geometry)

        if not coordinates:
            errors.append(f"Pas de coordonnées pour {code} ({nom})")
            continue

        bbox = compute_bbox(coordinates)

        if bbox:
            bbox_dict[code] = {
                "name": nom,
                "bbox": bbox,
            }

    # Afficher les erreurs
    if errors:
        print(f"\n⚠️ {len(errors)} erreur(s) :")
        for err in errors[:5]:
            print(f"   - {err}")
        if len(errors) > 5:
            print(f"   ... et {len(errors) - 5} autres")

    # Trier par code et sauvegarder
    bbox_dict_sorted = dict(sorted(bbox_dict.items()))

    print(f"\n💾 Sauvegarde dans : {output_path.name}")

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(bbox_dict_sorted, f, ensure_ascii=False, indent=2)

    print(f"✅ {len(bbox_dict)} bounding boxes générées")

    # Aperçu
    print("\n📋 Aperçu (5 premiers départements) :")
    for i, (code, data) in enumerate(list(bbox_dict_sorted.items())[:5]):
        bbox = data["bbox"]
        print(f"   {code} - {data['name']:25} : {bbox}")

    return True


# =============================================================================
# VÉRIFICATION DES FICHIERS
# =============================================================================

def check_reference_files() -> dict:
    """
    Vérifie que tous les fichiers de référence sont présents.

    Returns:
        Dict {nom: bool} pour chaque fichier
    """
    print("=" * 70)
    print("   VÉRIFICATION DES FICHIERS DE RÉFÉRENCE")
    print("=" * 70)
    print()

    files_to_check = {
        "departements_geojson": {
            "path": Config.GEOJSON_DEPT_FILE,
            "description": "Contours des départements (GeoJSON)",
            "source": "https://france-geojson.gregoiredavid.fr/",
            "required": True,
        },
        "codes_postaux_france": {
            "path": Config.CP_REFERENCE_FILE,
            "description": "Base des codes postaux France",
            "source": "https://datanova.laposte.fr/datasets/laposte-hexasmal",
            "required": True,
        },
        "bbox_pays": {
            "path": Config.BBOX_PAYS_FILE,
            "description": "Bounding boxes des pays",
            "source": "Fichier manuel (bbox_pays.json)",
            "required": True,
        },
        "bbox_departements": {
            "path": Config.BBOX_DEPT_FILE,
            "description": "Bounding boxes des départements (GÉNÉRÉ)",
            "source": "Généré par generate_bbox_departements()",
            "required": False,  # Sera généré si absent
        },
    }

    results = {}

    for key, info in files_to_check.items():
        path = info["path"]
        exists = path.exists()
        results[key] = exists

        if exists:
            size = path.stat().st_size
            size_str = (
                f"{size / 1_000_000:.1f} Mo" if size > 1_000_000
                else f"{size / 1_000:.1f} Ko"
            )
            print(f"✅ {info['description']}")
            print(f"   → {path.name} ({size_str})")
        else:
            marker = "❌" if info["required"] else "⚠️"
            print(f"{marker} {info['description']} MANQUANT")
            print(f"   → Attendu : {path}")
            print(f"   → Source  : {info['source']}")
        print()

    return results


# =============================================================================
# SETUP COMPLET
# =============================================================================

def run_setup() -> bool:
    """
    Exécute le setup complet :
    1. Vérifie les fichiers source
    2. Génère les fichiers dérivés si nécessaire
    3. Vérifie que tout est prêt

    Returns:
        True si tout est prêt, False sinon
    """
    print("\n" + "=" * 70)
    print("   SETUP — PRÉPARATION DES DONNÉES DE RÉFÉRENCE")
    print("=" * 70 + "\n")

    # Créer les répertoires
    for dir_path in [Config.INPUT_DIR, Config.REFERENCE_DIR, Config.OUTPUT_DIR]:
        dir_path.mkdir(parents=True, exist_ok=True)

    # Étape 1 : Vérifier les fichiers source
    status = check_reference_files()

    # Vérifier les fichiers source critiques
    source_files_ok = all(
        status.get(k, False)
        for k in ["departements_geojson", "codes_postaux_france", "bbox_pays"]
    )

    if not source_files_ok:
        print("❌ Setup interrompu : fichiers source manquants")
        print("   Téléchargez les fichiers manquants avant de continuer.")
        return False

    # Étape 2 : Générer bbox_departements.json si absent
    if not status.get("bbox_departements", False):
        print("🔄 Génération de bbox_departements.json...\n")
        success = generate_bbox_departements()
        if not success:
            print("\n❌ Setup interrompu : erreur lors de la génération")
            return False
        print()

    # Résumé
    print("=" * 70)
    print("✅ SETUP TERMINÉ — TOUTES LES DONNÉES SONT PRÊTES")
    print("=" * 70)

    return True
