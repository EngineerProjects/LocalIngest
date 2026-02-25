Projet : Outil d'automatisation pour le contrôle qualité des données Proxima
Document : Architecture technique et plan de réalisation


OBJECTIF DU DOCUMENT
---------------------
Ce document décrit en détail ce qui sera réalisé dans le cadre du MVP, les choix techniques
retenus, l'architecture du pipeline, ainsi que les étapes de développement à suivre.


CHOIX TECHNOLOGIQUE : PANDAS VS SPARK
--------------------------------------
Le choix retenu est Pandas.

Spark est conçu pour des volumes massifs de données distribuées (dizaines de millions de lignes
sur plusieurs machines). Dans notre contexte, les données clients issues de Proxima représentent
un volume raisonnable (quelques dizaines à quelques centaines de milliers de lignes maximum).
Spark introduirait une complexité d'infrastructure inutile.

Pandas est donc plus adapté : plus simple à développer, plus facile à maintenir, et
largement suffisant pour le volume de données traité.


STACK TECHNOLOGIQUE
-------------------

Langage        : Python 3.x

Librairies principales :
  - pandas        : chargement, filtrage et manipulation des données CSV
  - requests      : appels HTTP vers l'API de géocodage
  - geopy         : calcul de distance entre deux coordonnées GPS
  - openpyxl      : génération du fichier Excel de sortie (rapport des anomalies)

API externe :
  - API Adresse du gouvernement français (api-adresse.data.gouv.fr)
      → gratuite, sans clé, fiable pour la France métropolitaine
      → retourne : coordonnées GPS, adresse normalisée, score de confiance (0 à 1)
  - Alternative si besoin (DOM-TOM, étranger) : Nominatim / OpenStreetMap
      → gratuit, mondial, légèrement moins précis


ARCHITECTURE DU PIPELINE
-------------------------

Le pipeline est composé de 5 étapes séquentielles :

  [Étape 1] Chargement et filtrage des données
  --------------------------------------------
  - Lire le fichier CSV issu de Proxima avec Pandas.
  - Filtrer uniquement les contrats actifs :
      • cdsitp = 1
      • dtresilp vide (pas de date de résiliation renseignée)
  - Les contrats résiliés sont exclus définitivement du traitement.

  [Étape 2] Validation des adresses via géocodage
  ------------------------------------------------
  - Pour chaque ligne du fichier filtré, envoyer l'adresse à l'API Adresse (data.gouv.fr).
  - L'API retourne :
      • un score de confiance entre 0 et 1
      • les coordonnées GPS correspondantes (latitude / longitude de référence)
      • l'adresse normalisée (ville, code postal corrigés)
  - Règle de décision :
      • Score >= seuil défini (ex : 0.6 ou 0.7) → adresse considérée valide
      • Score < seuil ou aucun résultat retourné → anomalie signalée
  - Remarque importante : on ne cherche pas à corriger ni à distinguer la cause de l'erreur
    (faute de frappe, ville inexistante, adresse inventée, etc.). Quelle que soit la raison,
    si l'API ne reconnaît pas l'adresse avec suffisance, elle est classée comme anomalie.
    C'est au souscripteur de corriger la donnée en aval.

  [Étape 3] Vérification des coordonnées GPS
  -------------------------------------------
  - Pour les adresses validées à l'étape 2 (score suffisant), comparer les coordonnées GPS
    présentes dans le fichier avec les coordonnées GPS retournées par l'API.
  - Utiliser geopy pour calculer la distance entre les deux points.
  - Si l'écart dépasse un seuil défini (ex : 50 km) → anomalie signalée.
  - Ce cas couvre notamment les situations où une adresse est attribuée à la mauvaise ville
    (ex : adresse à Lille mais coordonnées pointant sur Paris).
  - Vérification complémentaire : s'assurer que les valeurs GPS du fichier sont dans
    des plages réalistes (latitude entre 41 et 51, longitude entre -5 et 10 pour la
    France métropolitaine).

  [Étape 4] Validation des données techniques
  --------------------------------------------
  - Vérifier qu'au moins un des deux champs suivants est renseigné : capitaux OU superficie.
  - Si les deux sont vides → anomalie signalée.
  - Si le temps le permet : vérifier la présence du statut juridique (locataire, propriétaire).

  [Étape 5] Génération du rapport Excel
  --------------------------------------
  - Consolider toutes les anomalies détectées aux étapes 2, 3 et 4.
  - Générer un fichier Excel horodaté (ex : rapport_anomalies_2025-04.xlsx).
  - Structure minimale du fichier :
      • Identifiant du point de gestion
      • Type d'anomalie (adresse non reconnue / écart GPS / données techniques manquantes)
      • Valeur problématique constatée
      • Date d'analyse
  - Une ligne = une anomalie détectée.
  - Les lignes sont classées par point de gestion.


SCHÉMA SIMPLIFIÉ DU PIPELINE
------------------------------

  Fichier CSV Proxima
         |
         v
  [1] Chargement & filtrage    (Pandas)
         |
         v
  [2] Géocodage des adresses   (API Adresse data.gouv.fr + requests)
         |
         |-- Score insuffisant → ANOMALIE : adresse non reconnue
         |
         v
  [3] Comparaison GPS          (geopy)
         |
         |-- Écart trop grand  → ANOMALIE : incohérence géographique
         |-- Valeurs hors plage → ANOMALIE : coordonnées GPS invalides
         |
         v
  [4] Validation technique     (Pandas)
         |
         |-- Capitaux ET superficie vides → ANOMALIE : données techniques manquantes
         |
         v
  [5] Génération rapport Excel (openpyxl)
         |
         v
  rapport_anomalies_AAAA-MM.xlsx


POINTS À CLARIFIER AVANT DE DÉMARRER
--------------------------------------
  1. Périmètre géographique : uniquement France métropolitaine, ou inclut DOM-TOM / étranger ?
     (conditionne le choix ou la combinaison d'API à utiliser)
  2. Structure exacte du fichier CSV : noms de colonnes, format des champs adresse et GPS.
  3. Seuils à calibrer sur données réelles :
     - Score de confiance de l'API en dessous duquel une adresse est considérée invalide.
     - Distance GPS maximale tolérée avant de signaler une incohérence.
  4. Récupérer le travail existant de Yanis sur les adresses (via Lionel) pour s'en inspirer.


PLAN DE DÉVELOPPEMENT (2 mois)
--------------------------------

  Semaines 1-2  : Exploration et cadrage
                  - Analyser la structure du CSV et identifier les colonnes utiles.
                  - Tester l'API Adresse manuellement sur quelques exemples.
                  - Récupérer et étudier le travail de Yanis.
                  - Définir les seuils initiaux (score, distance GPS).

  Semaines 3-5  : Développement des étapes 1, 2 et 3
                  - Chargement, filtrage, géocodage, comparaison GPS.

  Semaines 6-7  : Développement des étapes 4 et 5
                  - Validation technique (capitaux/superficie).
                  - Génération du fichier Excel de sortie.

  Semaine 8     : Tests, ajustements et documentation
                  - Tests sur données réelles.
                  - Ajustement des seuils si nécessaire.
                  - Documentation du code pour la passation.