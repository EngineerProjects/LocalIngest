Silde 1:

Bonjour à tous.Je suis ravi de vous présenter aujourd'hui le bilan deu projet que j'ai réalisé au sein de l'équipe ADC durant ces derniers mois.

Mon travail a porté sur un enjeu stratégique pour l'équipe (Allianz France) qui est la migration du Datamart Construction, depuis SAS vers une architecture sous Python et PySpark.

L'objectif de cette présentation est donc, de vous montrer très concrètement comment nous avons relevé ce défi. C'est parti !

Slide 2:

Pour commencer, pourquoi ce projet ?
Très simplement, il s'inscrit directement dans le cadre du projet global "SAS Exit" d'Allianz France, qui vise à quitter cette technologie sous licence qui est devenue beaucoup trop coûteuse et rigide.
L'objectif du projet était donc de s'aligner sur la vision Data du groupe en migrant vers un nouvel environnement Databricks sous PySpark. Cela nous permet de gagner considérablement en performance tout en réduisant drastiquement les coûts de maintenance.

Slide 3:

Pour mener à bien ce projet, nous avons adopté une méthodologie rigoureuse en 5 étapes. 
Notre conviction principale, et c'est le message clé ici : c'était de prendre le temps d'analyser et de documenter exhaustivement l'existant, AVANT d'écrire la moindre ligne de code. L'ancien code SAS n'était pas documenté, on ne pouvait pas y aller à l'aveugle.
Concrètement : on a d'abord compris l'existant (1) en l'analysant, puis on a recensé (2) toutes les règles métier et les datasets. 
Ce n'est qu'après ça que nous avons conçu (3) notre architecture cible, que l'on appelle "architecture médaillon".
Et pour finir, nous avons pu développer (4) sereinement les pipelines Python, et passer à la phase de validation (5) pour s'assurer que nos résultats étaient parfaitement identiques à ceux de SAS.

Slide 4:

En parlant de validation, c'était un point crucial du projet : garantir la stricte non-régression.
Pour y arriver, nous avons procédé sur 3 niveaux.
Premier niveau, le "Structurel" (1) : on a vérifié qu'on avait bien lu les 45 datasets d'entrée, on a contrôlé nos schémas de données et nos types.  
Ensuite, le "Fonctionnel" (2), c'est le cœur de la recette : on a comparé nos indicateurs KPI avec la sortie SAS sur 5 mois différents. Notre cible était exigeante : on voulait moins de 0.01% d'écart.
Et enfin, la "Performance" (3) : On s'est assuré que nos temps de traitements étaient bons et on a optimisé notre consommation mémoire pour être efficace sur l'ADP.

Slide 5:

On passe maintenant aux résultats concrets de la migration. 
Comme je vous l'ai dit en méthodologie, la première étape a été d'analyser le code SAS. Cela nous a permis de cartographier son architecture globale, que vous voyez ici.
Pour faire très bref : on part de nos 3 sources de données à gauche (IMS, OneBI, Référentiels), on applique nos différentes transformations au centre, pour finalement alimenter 3 pipelines métiers distincts : Capitaux, Portefeuille et Émissions, qui produisent chacun leurs propres livrables analytiques.
Tout le détail de cette architecture et des règles associées est bien sûr documenté et disponible dans nos livrables.

Slide 6:

Pour entrer un peu plus dans le vif du sujet de cette phase d'analyse préalable, voici un aperçu de ce que ça donne. 
D'un côté, on a la documentation SAS : j'ai pu auditer 19 fichiers historiques complexes pour en sortir ces schémas et documentations.
De l'autre, le travail sur les données elles-mêmes : on a dû identifier, valider et vérifier 45 datasets sources différents (des tables IPF, des référentiels, etc.). Ce travail minutieux en amont nous a notamment permis de détecter très tôt certains fichiers qui allaient nous manquer dans le datalake, et donc d'éviter de bloquer nos développements plus tard.

Slide 7:

Dans la même logique de préparation, nous avons formalisé un dictionnaire exhaustif de *toutes* les règles métier. C'est ce que vous voyez ici.
C'est un travail de fourmi : on a épluché le code pour identifier 72 règles de gestion précises.
Pour chaque règle, on a documenté la variable SAS d'origine et on a fait le mapping direct vers la fonction Python qu'on allait devoir développer. C'est la base qui nous a permis de coder proprement sans rien oublier.

Slide 8:

Une fois l'analyse terminée, nous avons défini notre nouvelle architecture cible : l'Architecture Médaillon.
C'est tout simplement la norme d'architecture la plus adaptée à notre nouvel outil, Databricks.
L'idée est très simple à comprendre pour tout le monde, on avance en 3 grandes étapes :
- "Bronze" : on stocke nos données brutes telles quelles pour en garder une trace et pouvoir retrouver toutes nos erreurs.
- "Silver" : on va venir nettoyer et filtrer tout ça.
- "Gold" : on fait finalement nos calculs finaux et nos agrégations prêts à être consommés par les métiers.
C'est une structure très claire, performante et totalement pensée pour un Datamart de ce type sur le cloud.

Slide 9:

Concrètement, qu'est-ce que nous avons développé au sein de cette architecture ? 
Nous avons réécrit les 3 pipelines majeurs de notre datamart :
1. Le **PTF Mouvements**, qui nous permet de suivre toute la vie des contrats mois par mois, des affaires nouvelles jusqu'aux résiliations.
2. Les **Capitaux**, pour évaluer très précisément notre exposition au risque sur les différentes garanties.
3. Et les **Émissions**, qui calculent et ventilent le chiffre d'affaires.

Pour que tout ce code soit propre et surtout réutilisable à l'avenir pour d'autres marchés, on a structuré notre projet Python de manière très modulaire : avec un dossier pour la config centralisée, un pour les fonctions utilitaires communes (comme la lecture ou l'écriture), et un dossier "src" réservé à la logique métier pure de chaque pipeline.

Slide 10:

Pour vérifier que ces pipelines fonctionnent parfaitement, passons aux résultats de validation. Je vous montre ici l'exemple du pipeline PTF Mouvements.
Comme vous le voyez sur ce tableau et le log à droite, la vérification est sans appel : sur plus de 87 000 polices d'assurance traitées pour cette vision, nous avons exactement le même nombre dans SAS et dans Python. 
Zéro police manquante, zéro police en trop, et 100% des colonnes sont identiques. Le pourcentage de différence est de 0,00%. Les deux systèmes sont strictement alignés.

Slide 11:

Et ce n'est pas seulement vrai au global, c'est aussi vrai dans le détail de chaque indicateur métier.
Sur cette slide, on compare la sortie SAS et Python pour nos 4 grands KPIs : le nombre de portefeuilles (NBPTF), les affaires nouvelles (NBAFN), les résiliations (NBRES) et le nombre de polices (NOPOL), tout ça ventilé par produit.
Et vous pouvez voir dans la colonne de droite, peu importe le produit ou l'indicateur qu'on regarde : l'écart est toujours de zéro. On a rempli notre objectif de validation fonctionnelle à 100%.

Slides 14 & 15:

Et pour ne pas m'attarder trop longtemps sur les chiffres, je vous affiche très brièvement les résultats de validation pour le pipeline des Capitaux.
Comme vous pouvez le constater, le verdict est exactement le même que pour les Mouvements de Portefeuille : nous avons une correspondance stricte de 100%. La non-régression est totalement garantie ici aussi, jusque dans le détail des montants assurés par produit.

Slide 16:

Au-delà de l'exactitude des données, l'autre grand enjeu de cette migration, c'était la performance. Et là aussi, le contrat est rempli.
En comparant les temps de traitement globaux de nos 3 pipelines métiers (PTF Mvt, Capitaux, Emissions), la nouvelle architecture Python sous PySpark s'est révélée 3 fois plus rapide que l'ancien système SAS.
Concrètement, on observe une réduction moyenne des temps d'exécution de 66%. C'est un gain d'efficacité particulièrement notable pour l'équipe ADC au quotidien.

Slide 17:

Pour clôturer sur les résultats de ce projet, voici le bilan de tout ce qui a été produit et livré à l'équipe.
On ne laisse pas seulement du code derrière nous, on laisse un package complet.
Il y a évidemment le **Code Source** Python, validifiées et poussées sur Github.
Mais il y a aussi toute la **Documentation** technique, très précieuse puisqu'elle n'existait pas avant, nos **Résultats d'Analyse** avec tous les mappings de variables, et enfin les cahiers de **Validation** qui prouvent l'exactitude de notre travail. C'est ce corpus complet qui pérennise la migration.

Slide 18:

*Option 1 (Très naturel et direct, moins "scolaire") :*
Pour terminer, je voulais juste faire un petit bilan personnel. Ces derniers mois ont été super enrichissants pour moi.
Techniquement bien sûr, avec la découverte de PySpark sur un vrai cas d'usage assez complexe. Mais aussi sur l'organisation : j'ai vraiment appris à gérer un vrai projet de A à Z et à prioriser mes tâches au quotidien.
Si on a pu aboutir à un super résultat aujourd'hui, c'est grâce à l'accompagnement de toute l'équipe ADC. Merci pour votre temps et votre confiance, je suis très fier d'avoir fait partie de l'équipe pour ce travail.

*Option 2 (Plus courte, orientée remerciements) :*
Pour terminer, d'un point de vue plus personnel, ce projet a été une superbe opportunité pour moi. J'ai pu monter en compétence sur la stack PySpark, mais j'ai surtout appris à m'organiser pour mener un vrai projet à son terme.
Rien de tout ça n'aurait été possible sans les conseils et le temps que toute l'équipe m'a accordé. Je vous remercie vraiment pour la confiance que vous m'avez témoignée depuis mon arrivée, je suis très fier du chemin qu'on a parcouru ensemble.
