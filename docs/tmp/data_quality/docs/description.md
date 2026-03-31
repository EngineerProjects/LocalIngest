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