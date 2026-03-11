# Colonnes d'entrée et de sortie
*Document de travail — spécification des données*

---

## Colonnes d'entrée

Sur les 154 colonnes du CSV Proxima, seules 25 sont utilisées par le projet.
Les 129 autres sont ignorées lors du chargement.

### Identification & filtre

| Colonne | Type | Rôle | Règle |
|---|---|---|---|
| `STOCK` | float | Filtre contrat actif (garder uniquement = 1) | Filtre |
| `DT_EFF_RESIL_CNT` | str (date YYYY-MM-DD) | Filtre contrat non résilié (garder si vide) | Filtre |
| `NU_CNT` | int | Numéro de contrat | Identification |
| `PTGST` | str | Point de gestion | Identification |
| `ID_SITE` | float | Identifiant du site — clé principale du rapport | Identification |
| `NOM_CLI` | str | Nom du client | Identification |
| `NOM_SITE` | str | Nom du site | Identification |
| `marche` | str | Marché (PART, RI&GAR…) | Identification |

### Adresse

| Colonne | Type | Rôle | Règle |
|---|---|---|---|
| `NUM_VOIE_SITE` | float | Numéro de voie | R1 |
| `RUE_SITE` | str | Nom de la rue | R1 |
| `NUM_RUE_SITE` | str | Numéro + rue concaténés | R1 |
| `LIEU_DIT_SITE` | str | Complément d'adresse / lieu-dit | R1 |
| `CP_SITE` | float | Code postal (attention : zéro initial perdu, ex: 4100.0 = 04100) | R1, R2, R3 |
| `VILLE_SITE` | str | Ville | R1, R3 |
| `PAYS_SITE` | str | Pays | R1, R2, R4 |

### GPS

| Colonne | Type | Rôle | Règle |
|---|---|---|---|
| `COORD_X_SITE` | float | Longitude | R4 |
| `COORD_Y_SITE` | float | Latitude | R4 |
| `QUALITE_GEOCODAGE` | str | Qualité du géocodage — information complémentaire | R4 |

### Données techniques

| Colonne | Type | Rôle | Règle |
|---|---|---|---|
| `SURFACE` | float | Superficie principale | R5 |
| `SURFACE_DPDCE` | float | Superficie dépendances | R5 |
| `CAPITAUX_MURS_BATIMENTS` | float | Capitaux murs & bâtiments | R5 |
| `CAPITAUX_CONTENU` | float | Capitaux contenu | R5 |
| `CAPITAUX_MATERIEL` | float | Capitaux matériel | R5 |
| `CAPITAUX_MARCHANDISES` | float | Capitaux marchandises | R5 |
| `CAPITAUX_TT` | float | Capitaux total *(à confirmer : toujours 0 dans l'échantillon)* | R5 |

> ⚠️ **Colonne à venir via le datamart :** `CDSITP` — statut actif du contrat (= 1 si actif).
> Non présente dans le CSV Proxima, elle sera disponible après fusion avec le datamart.

---

## Colonnes de sortie

Le fichier de sortie est généré à chaque exécution avec un horodatage dans le nom
(ex: `rapport_anomalies_2026-03-06.xlsx`).

**Une ligne = une anomalie. Un site avec plusieurs anomalies apparaît sur plusieurs lignes.**

| Colonne | Type | Exemple | Description |
|---|---|---|---|
| `date_analyse` | date | 2026-03-06 | Date d'exécution du script |
| `id_site` | int | 1651 | Identifiant du site *(clé principale — plus précis que PTGST car un contrat peut avoir plusieurs sites)* |
| `ptgst` | str | N30 | Point de gestion — conservé pour le rapprochement métier |
| `nu_cnt` | int | 59375444 | Numéro de contrat |
| `nom_cli` | str | SOC RAIGI | Nom du client |
| `nom_site` | str | ROS | Nom du site |
| `marche` | str | PART | Marché |
| `code_anomalie` | str | R1-03 | Code de la règle enfreinte |
| `libelle_anomalie` | str | CP ou Ville manquant | Description lisible de l'anomalie |
| `gravite` | str | Grave | `Grave` ou `Légère` |
| `colonne_concernee` | str | CP_SITE | Colonne source du problème |
| `valeur_problematique` | str | NaN | Valeur qui pose problème |
| `statut` | str | Nouveau | `Nouveau`, `Persistant` ou `Corrigé` *(comparaison avec le fichier du mois précédent)* |

---

## Référentiel des codes anomalies

| Code | Règle | Libellé | Gravité |
|---|---|---|---|
| R1-01 | Règle 1 | Adresse entièrement manquante | Grave |
| R1-02 | Règle 1 | Adresse incomplète (rue absente) | Légère |
| R1-03 | Règle 1 | CP ou Ville manquant | Grave |
| R1-04 | Règle 1 | Pays manquant | Grave |
| R2-01 | Règle 2 | Format code postal invalide | Grave |
| R2-02 | Règle 2 | Code postal inexistant | Grave |
| R3-01 | Règle 3 | Incohérence CP / Ville | Légère |
| R4-01 | Règle 4 | GPS incohérent avec l'adresse (hors bounding box département) | Légère |
| R4-02 | Règle 4 | GPS hors du pays renseigné | Grave |
| R5-01 | Règle 5 | Données techniques manquantes (capitaux et superficie absents) | Grave |
