Projet : Mise en place d'un outil d'automatisation pour le contrôle qualité des données issues de Proxima


OBJECTIF GÉNÉRAL
----------------
Développer un outil capable d'analyser, valider et détecter automatiquement les anomalies présentes
dans les données clients récupérées via Proxima. L'enjeu est d'assurer la cohérence, la complétude
et l'exploitabilité des informations avant leur utilisation dans les processus métiers.


PÉRIMÈTRE DU PROJET
-------------------

1. Sources de données
   - Récupération des données clients à partir de fichiers CSV.
   - Fusion (merge) avec différents datamarts afin de compléter les informations manquantes.
     Note : il n'existe pas de datamart dédié pour les groupements sur ce marché.

2. Vérifications à effectuer

   a. Validité du contrat
      - Un contrat est considéré comme actif si les deux conditions suivantes sont réunies :
        • cdsitp = 1
        • dtresilp est vide (aucune date de résiliation renseignée)
      - Les contrats déjà résiliés sont exclus du périmètre de contrôle qualité.

   b. Cohérence et exploitabilité des informations

      - Vérification des données géographiques :
        • Adresses et codes postaux : validité, existence, correspondance avec le pays attendu.
        • Coordonnées GPS (latitude, longitude) : cohérence, positionnement correct, détection d'erreurs.

      - Vérification des données techniques :
        • Présence des informations essentielles : capitaux OU superficie
          (il n'est pas nécessaire d'avoir les deux champs renseignés, un seul suffit).
        • Détection des dossiers incomplets ou impossibles à exploiter.
        • Statut juridique (locataire, propriétaire, etc.) : à traiter uniquement si le temps le permet.

3. Suivi dans le temps
   - Mise en place d'un mécanisme permettant de vérifier, mois après mois, si les contrats déjà
     analysés ont été traités ou s'ils présentent de nouvelles anomalies.


MVP À RÉALISER
--------------

Dans le cadre des deux mois de stage restants, le MVP se concentre sur l'essentiel et exclut
toute interface visuelle ou tableau de bord complexe.

1. Chargement et filtrage des données
   - Lire le fichier CSV issu de Proxima.
   - Filtrer uniquement les contrats actifs (cdsitp = 1 et dtresilp vide).
   - Ignorer les contrats résiliés.

2. Vérifications à implémenter (par ordre de priorité)

   Priorité 1 — Adresses et géographie (sujet principal)
   - Vérification de la présence des champs adresse (champ vide ou null).
   - Validation du format du code postal et cohérence avec le pays.
   - Vérification des coordonnées GPS : présence, plage de valeurs réaliste,
     cohérence avec le code postal.

   Priorité 2 — Données techniques
   - Vérification qu'au moins un des deux champs suivants est renseigné : capitaux ou superficie.

   Priorité 3 — Statut juridique (si le temps le permet)
   - Vérification de la présence et de la validité du statut juridique (locataire, propriétaire, etc.).

3. Livrable de sortie
   - Un fichier Excel généré automatiquement à chaque exécution.
   - Une ligne par anomalie détectée, classée par point de gestion.
   - Colonnes minimales : identifiant du point de gestion, type d'anomalie, valeur problématique,
     date d'analyse.

4. Suivi mensuel (version simplifiée)
   - Chaque exécution génère un fichier horodaté.
   - Comparaison possible entre deux fichiers pour identifier les anomalies corrigées ou nouvelles
     d'un mois à l'autre (peut rester manuel dans cette première version).

5. Hors périmètre MVP
   - Interface visuelle ou tableau de bord interactif.
   - Fusion avec un datamart groupement (hors scope confirmé).
   - Automatisation complète du suivi mensuel (prévu en version ultérieure).