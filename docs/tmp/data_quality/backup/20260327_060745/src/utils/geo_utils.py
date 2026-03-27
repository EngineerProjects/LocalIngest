"""
Utilitaires géographiques.

- extract_department : extraction code département depuis CP français
- extract_region_code : dérivation du code région/département selon le pays
- is_in_bbox : vérification point dans bounding box
- extract_all_coordinates : extraction coords depuis GeoJSON
- compute_bbox : calcul bounding box depuis liste de coords
"""

from typing import Callable, Dict, List, Optional, Tuple

from src.utils.string_utils import normalize_country, safe_str


def extract_department(cp: str) -> str:
    """
    Extrait le code département d'un code postal français.
    Gère les cas spéciaux (Corse, DOM-TOM).
    """
    cp = safe_str(cp).zfill(5)
    if len(cp) < 2:
        return ""
    # DOM-TOM : 3 premiers chiffres
    if cp[:3] in ["971", "972", "973", "974", "976"]:
        return cp[:3]
    # Corse : 20 → 2A ou 2B
    if cp[:2] == "20":
        if len(cp) >= 3:
            return "2A" if cp[2] in "012" else "2B"
        return "20"
    # Cas général
    return cp[:2]


def extract_region_code(country: str, cp: str) -> str:
    """
    Dérive un code région/département depuis un CP quand une règle pays existe.

    Retourne "" si aucune règle fiable n'est disponible pour ce pays.
    """
    extractors: Dict[str, Callable[[str], str]] = {
        "FRANCE": extract_department,
    }
    extractor = extractors.get(normalize_country(country))
    if extractor is None:
        return ""
    return extractor(cp)


def is_in_bbox(lon: float, lat: float, bbox: List[float]) -> bool:
    """
    Vérifie si un point (lon, lat) est dans une bounding box.

    Args:
        bbox: [lon_min, lat_min, lon_max, lat_max]
    """
    return bbox[0] <= lon <= bbox[2] and bbox[1] <= lat <= bbox[3]


def extract_all_coordinates(geometry: dict) -> List[Tuple[float, float]]:
    """
    Extrait toutes les coordonnées d'une géométrie GeoJSON.
    Gère Polygon et MultiPolygon.

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
        print(f"Type de géométrie non supporté : {geom_type}")

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
        round(min(lons), 4),
        round(min(lats), 4),
        round(max(lons), 4),
        round(max(lats), 4),
    ]
