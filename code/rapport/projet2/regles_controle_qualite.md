# Règles de contrôle qualité — Données Proxima
*Document de travail — rédigé lors de la phase d'analyse*

---

## Contexte

Les données sources sont des fichiers CSV issus de Proxima, fusionnés avec un datamart.
Chaque ligne représente un site (point de gestion). Un contrat peut avoir plusieurs sites.
Le contrôle qualité s'applique ligne par ligne, indépendamment.

---

## Filtre préalable — Contrats actifs

Avant toute vérification, on ne traite que les lignes correspondant à des sites actifs.

**Condition d'activité (dans le CSV Proxima) :**
- `STOCK = 1`

**Condition complémentaire (après fusion avec le datamart) :**
- `CDSITP = 1`
- `DT_EFF_RESIL_CNT` est vide (pas de date de résiliation)

> ⚠️ **Point à confirmer avec Alain** : est-ce que `STOCK = 1` suffit seul à garantir qu'un contrat
> est actif, ou faut-il obligatoirement le `CDSITP` du datamart pour être certain ?

---

## Règle 1 — Présence des champs d'adresse

### Colonnes concernées

| Colonne | Rôle |
|---|---|
| `NUM_VOIE_SITE` | Numéro de voie (ex: 7) |
| `RUE_SITE` | Nom de la rue |
| `NUM_RUE_SITE` | Numéro + rue concaténés (ex: "7 RUE SIMON CASTAN") |
| `LIEU_DIT_SITE` | Complément d'adresse / lieu-dit |
| `CP_SITE` | Code postal |
| `VILLE_SITE` | Ville |
| `PAYS_SITE` | Pays |

### Logique de vérification

**Sur le champ rue :**
`RUE_SITE` et `NUM_RUE_SITE` sont redondants (le second contenant le premier).
- Si les deux sont présents → utiliser `NUM_RUE_SITE` en priorité (à confirmer).
- Si seulement l'un est présent → utiliser celui disponible.

> ⚠️ **Question à poser à Alain** : lequel de `RUE_SITE` ou `NUM_RUE_SITE` est le champ
> officiel de référence dans Proxima ?

**Sur la combinaison minimale valide :**

| Situation | Statut | Type d'anomalie |
|---|---|---|
| `CP_SITE` + `VILLE_SITE` + un champ rue présents | ✅ Valide | — |
| `CP_SITE` + `VILLE_SITE` présents, rue absente | ⚠️ Anomalie légère | Adresse incomplète |
| `CP_SITE` absent OU `VILLE_SITE` absent | ❌ Anomalie grave | Adresse inexploitable |
| Tous les champs adresse absents | ❌ Anomalie grave | Adresse manquante |

> ⚠️ **Question à poser à Alain** : valider cette gradation légère / grave. Est-ce pertinent
> métier de distinguer ces deux niveaux, ou tout doit être traité comme anomalie bloquante ?

**Sur `PAYS_SITE` :**
Le pays doit toujours être renseigné. Sans pays, on ne peut pas valider le format du code postal
ni les coordonnées GPS.
- Si `PAYS_SITE` est vide → anomalie "Pays manquant".

---

## Règle 2 — Format et validité du code postal

### Colonnes concernées
- `CP_SITE`
- `PAYS_SITE`

### Logique de vérification

**Pré-traitement nécessaire :**
`CP_SITE` est stocké en float dans le CSV (ex: `4100.0`).
Il faut le convertir en string et repadder à 5 chiffres (ex: `04100`).

**Pour la France (`PAYS_SITE = "FRANCE"`) :**
1. Le code postal doit contenir exactement 5 chiffres numériques.
2. Les 2 premiers chiffres (le département) doivent correspondre à un département existant :
   - Métropole : 01 à 95 (sauf 20, remplacé par 2A et 2B)
   - DOM-TOM : 971, 972, 973, 974, 976
3. Si le CP ne respecte pas ce format → anomalie "Format code postal invalide".
4. Si le département déduit n'existe pas → anomalie "Code postal inexistant".

**Pour les autres pays :**
Appliquer le format de code postal propre au pays concerné.
La logique est extensible : chaque pays a ses propres règles de validation.

> ℹ️ Les données sont majoritairement françaises mais d'autres pays sont possibles.
> La version initiale peut se concentrer sur la France et traiter les autres pays
> comme "format non validé" (sans anomalie bloquante) dans un premier temps.

---

## Règle 3 — Cohérence code postal / ville

### Colonnes concernées
- `CP_SITE`
- `VILLE_SITE`

### Source de référence
Base officielle des codes postaux français (La Poste / data.gouv.fr) — fichier libre et gratuit.
Pour chaque CP, elle liste les communes associées.

### Logique de vérification

La règle 3 s'applique différemment selon la présence ou l'absence de chaque champ :

| Scénario | CP_SITE | VILLE_SITE | Action |
|---|---|---|---|
| A | ✅ Présent | ✅ Présente | Vérification complète : existence du CP + cohérence avec la ville |
| B | ✅ Présent | ❌ Absente | Anomalie déjà levée par Règle 1. Ici : valider le CP seul (existence dans la base) |
| C | ❌ Absent | ✅ Présente | Anomalie déjà levée par Règle 1. Règle 3 non applicable sans CP |
| D | ❌ Absent | ❌ Absente | Anomalie grave déjà levée par Règle 1. Règle 3 non applicable |

**Détail du scénario A (cas normal) :**
1. Chercher le `CP_SITE` dans la base officielle.
2. Si le CP n'existe pas dans la base → anomalie "CP inexistant".
3. Si le CP existe, vérifier que `VILLE_SITE` correspond à l'une des communes associées.
4. Si la ville ne correspond pas → anomalie "Incohérence CP / Ville".

**Détail du scénario B (CP présent, ville absente) :**
1. Chercher le `CP_SITE` dans la base officielle.
2. Si le CP n'existe pas → anomalie "CP inexistant".
3. Si le CP existe → pas d'anomalie supplémentaire sur la règle 3 (la ville manquante est déjà remontée par la règle 1).

**Normalisation pour la comparaison :**
- Convertir en minuscules des deux côtés.
- Supprimer les espaces superflus.
- Les accents : pas de correction automatique dans cette version (trop complexe).
  Une ville comme "Rouen" vs "rouen" sera tolérée, mais "Rouen" vs "Roen" sera une anomalie.
- Les fautes de frappe évidentes (ex: "LiLLLe") sont traitées comme anomalies.

> ℹ️ Amélioration future possible : utiliser une distance de Levenshtein pour tolérer
> les petites erreurs de saisie, ou une API de géocodage (ex: API Adresse du gouvernement).

---

## Règle 4 — Coordonnées GPS

### Colonnes concernées
- `COORD_X_SITE` (longitude)
- `COORD_Y_SITE` (latitude)
- `CP_SITE`, `VILLE_SITE`, `RUE_SITE` (pour les scénarios avec adresse)
- `PAYS_SITE`

### Trois scénarios

**Scénario A — Adresse présente, GPS absent**
- `RUE_SITE` / `CP_SITE` / `VILLE_SITE` renseignés + `COORD_X` et `COORD_Y` vides.
- → **Valide. Aucune anomalie.**
- L'adresse texte suffit pour localiser le site.

**Scénario B — Adresse présente ET GPS renseigné**
- Vérifier que les coordonnées sont cohérentes avec l'adresse.
- Méthode : comparer les coordonnées à la **bounding box du département** déduit du CP
  (les limites géographiques min/max lat-lon de ce département).
  - Bounding box = rectangle englobant tout le département.
  - Si les coordonnées tombent hors de ce rectangle → anomalie "GPS incohérent avec l'adresse".

> ℹ️ Rappel : le département = les 2 premiers chiffres du CP (ex: CP 69800 → département 69 → Rhône).
> La bounding box du département est une limite large mais suffisante pour détecter
> une vraie erreur (ex: un site censé être dans le Rhône avec des coordonnées en Bretagne).

**Scénario C — GPS renseigné, adresse absente**
- Vérifier que les coordonnées correspondent bien au pays renseigné dans `PAYS_SITE`.
- Pour la France : latitude entre 41.3° et 51.1°, longitude entre -5.1° et 9.6°.
- Si hors limites → anomalie "GPS hors du pays renseigné".
- Si `PAYS_SITE` est aussi absent → double anomalie (règle 1 + règle 4).

**Cas dégénéré — GPS et adresse tous les deux absents**
- → Anomalie grave couverte par la Règle 1 (adresse manquante).

---

## Règle 5 — Présence des données techniques (capitaux ou superficie)

### Colonnes concernées

Capitaux (plusieurs types possibles) :
- `CAPITAUX_MURS_BATIMENTS`
- `CAPITAUX_CONTENU`
- `CAPITAUX_MATERIEL`
- `CAPITAUX_MARCHANDISES`
- `CAPITAUX_TT` *(à confirmer : toujours à 0 dans l'échantillon, peut-être inutilisable)*

Superficie :
- `SURFACE`
- `SURFACE_DPDCE`

### Logique de vérification

Au moins **un** des champs suivants doit être renseigné et non nul :
- N'importe lequel des capitaux listés ci-dessus
- OU `SURFACE` / `SURFACE_DPDCE`

Si aucun n'est renseigné → anomalie "Données techniques manquantes (capitaux et superficie absents)".

> ⚠️ **Questions à poser à Alain :**
> 1. Est-ce qu'un seul type de capital suffit, ou certains sont obligatoires selon le type de bien ?
> 2. `CAPITAUX_TT` est-il un total calculé ou un champ à part entière ? Peut-on s'y fier ?
> 3. Y a-t-il d'autres colonnes capitaux non listées ici qui peuvent arriver via le datamart ?

---

## Bases de données de référence

### Par règle

**Règles 1** — Aucune base externe nécessaire. Vérification de nullité uniquement.

**Règles 2 & 3** — Une seule base couvre les deux règles :
- 🇫🇷 **France** : Base officielle des codes postaux (data.gouv.fr)
  - Contenu : CP → liste des communes associées
  - Accès : gratuit, téléchargement direct
  - URL : https://www.data.gouv.fr
- 🌍 **International** : Base GeoNames
  - Contenu : codes postaux + communes pour la quasi-totalité des pays du monde
  - Accès : gratuit, téléchargement direct
  - URL : https://www.geonames.org/postal-codes/

**Règle 4** — Deux besoins distincts :
- **Scénario B (bounding box par département)** : à construire nous-mêmes
  - Source : fichier GeoJSON des contours des départements français (data.gouv.fr)
  - Méthode : calcul automatique du min/max lat-lon pour chaque département
  - Résultat : un fichier de référence généré une seule fois et embarqué dans le projet
- **Scénario C (bounding box par pays)** : base existante
  - Source : GeoNames (limites géographiques par pays)
  - Accès : gratuit, téléchargement direct

### Récapitulatif

| Règle | Base nécessaire | Disponible ? | Comment l'obtenir |
|---|---|---|---|
| R2 + R3 | Codes postaux France | ✅ Gratuit | data.gouv.fr — téléchargement direct |
| R2 + R3 | Codes postaux international | ✅ Gratuit | GeoNames — téléchargement direct |
| R4 scénario B | Bounding boxes par département | ⚙️ À construire | Contours GeoJSON départements (data.gouv.fr) → calcul automatique |
| R4 scénario C | Bounding boxes par pays | ✅ Gratuit | GeoNames ou dataset country_bounding_boxes |

> ⚠️ **Question à poser à Alain** : quels sont les pays les plus fréquents après la France
> dans les données ? Cela permettra de prioriser la prise en charge internationale
> (si c'est surtout Belgique + Luxembourg, GeoNames suffit largement).

---

## Récapitulatif des anomalies

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

---

## Points en suspens (à clarifier avec Alain)

1. `STOCK = 1` suffit-il à identifier un contrat actif, ou faut-il obligatoirement le `CDSITP` du datamart ?
2. Quel champ est la référence pour la rue : `RUE_SITE` ou `NUM_RUE_SITE` ?
3. Valider la gradation anomalie légère / grave pour la règle 1.
4. Autres pays possibles dans `PAYS_SITE` : lesquels exactement ?
5. `CAPITAUX_TT` : champ calculé ou saisie directe ?
6. Pour la règle 5 : certains capitaux sont-ils obligatoires selon le type de bien ?
7. Y a-t-il d'autres colonnes capitaux qui arrivent via le datamart ?
