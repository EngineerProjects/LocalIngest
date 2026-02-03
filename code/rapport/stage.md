Je suis en stage chez Allianz France, dans la direction P4D qui est en charge de plusieurs autres poles dont je suis dplus précisement dans le pole Advanced Data & Climate.

Ce pole s'occupe principalement du data management et de la mise à disposition des données pour les autres poles de la direction P4D ainsi que des expertises transverses comme les recherches sur le climat, le maping et différentes sortes de modélisations. DOnc au sein de ce pole, il y a plusieurs équipes qui travaillent sur des sujets assez variés et importants pour Allianz.

L'équipe que j'intègre se charge de la mise à disposition des datamarts, mais aussi d'autres projet transverses.

A mon arrivé dans l'équipe, dès les premières semaines, j'ai d'abord commencer à rencontrer les différents membres du pôle où je travaille, afin de bien comprendre les différents sujets abordés par chacun et de comprendre comment fonctionnait l'équipe et comment elle s'intégrait dans la direction P4D. Donc la première a surtout été consacrée à la rencontre avec mes collègues et à la compréhension des différents sujets abordés par chacun, et aussi comprendre, ce qui est important à connaitre sur les BABA de l'assurance pour ne pas me perdre dans les différents sujets abordés au sein de l'équipe. Durant les deux premiers jours j'ai aussi reçu la charte informatique et les différents règlements necessaire à connaitre en tant qu'employé d'allianz.
Après la rencontre avec presque tous les membres du pôle, j'ai reçu mon ordinateur de travail le troisième jour de mon stage et aussi été accompagner par mon tuteur de stage pour les configurations necessaires et la mise en place de mon environnement de travail.
Et aussi j'ai donc pu demander les accès necessaires aux différents envirements. 
Donc le début de la semaine 2, j'ai donc eu une réunion avec mon tuteur de stage pour bien comprendre le projet sur lequel j'allais travailler. Il m'a donc présenter le projet dans son ensemble, son importance et le scope du projet, ainsi que comment je vais devoir travailler avec lui et tous les membres de l'équipe. 

Dans la même semaine, j'ai eu accès au platforme de formations d'allianz, qui m'a permis de suivre plusieurs formations en ligne pour bien comprendre les bases de l'assurance et les différents comportements à avoir au sein de l'entreprise. 


Le projet principal sur lequel je travaille est nommé : "SAS EXIST" et consiste à travailler principalement sur la migrations des datamarts du langage SAS vers PySpark. 
En gros l'équipe actuellement utilise, principal du sas pour tous  leurs travaux et métiers, en raison des prix des licenses, de la performance des serveurs, l'équipe a choisi de passer aux équipements plus modernes et plus performants, sur AZURE. d'où la naissance du projet "SAS EXIST".

Les datamarts sont des bases de données qui contiennent des données agrégées et transformées pour faciliter l'analyse et la prise de décision sur les différents types de contrats d'assurance d'allianz dans les différents secteurs ou marchés.

Mon stage constistera donc à créer le premier prototype de cette migration en utilisant bien sûr le datamart le plus simple à gérer qui est celui du marché construction. 

semaine 2 et 3 : 
Pour ce faire, j'ai du dans un premier temps comprendre le fonctionnement du datamart en analysant les différents programmes SAS qui constituent ce datamart afin de bien comprendre dans l'emsemble comment il fonctionnait et ce que faisait exactement ce datamart. Etant donné qu'il n'y avait pas de documentation existante sur le processus de création de ce datamart,  avec les vérifications régulieres de mon tuteur, j'ai créer une nouvelle documentation qui expliquait pricipalement l'architecture de ce datamart, les principales sources de données et les différentes étapes quie suivent le datamart construction. 
J'ai aussi recensé les grandes règles de gestion (qui sont importants dans ce datamart).

Durant cette semaine, j'ai pu rencontrer Alain, l'expert en assurance de notre équipe, qui m'a expliquer tout le dérouler sur les contrats d'assurance et à répondu aux question que j'avais à lui poser. Ce qui m'a permis de bien comprendre et refermer les petits zone flou que j'avais au début.


semaine 4 :

Après avoir compris en grande partie le fonctionnement du datamart, j'ai fais une deuxième étape d'analyse qui consistait surtout à énumérer avec détail toutes les règles de gestions qui sont appliquées dans le datamart construction ainsi que les différentes données qui entrent en jeu par étape et par processus.

Par la suite, j'ai discuter avec  mon tuteur de stage, de comment est ce que je devrais faire la version python de ce datamart, maintenant que j'en ai une compréhension et donc nous avons pu décider de l'architecture à suivre pour ce datamart qui est l'architecture médaille.

Dans cette même semaine, j'ai eu ma première réunion avec mon tuteur de stage et mon M+1 pour faire le point sur l'avancement de mon travail et pour discuter des différentes problématiques que j'ai pu rencontrer. Durant cette réunion, j'ai donc faire une présentation de mon travail avec un powerpoint et la documentation que j'ai réalisé pour présenter mon travail, mais aussi ce que j'ai pu comprendre de ce datamart. Et donc j'ai reçu les recommendations de mes deux tuteurs,s sur les parties à améliorer dans mon travail et ce que j'ai déjà bien fait.

semaine 5 :

Tenant compte de leurs remarques, la semaine qui a suivi, j'ai énumérer toutes les tables utilisés dans le datamart en version sas dans un tableau excel ainsi  que toutes les 72 et plus règles de gestion identifier dans le code.
J'ai alors commencer avec la version python du datamart construction, mettre en place le le format et l'architecture à suivre pour le package que j'aurai à créer, ainsi que la première base qui me permet de lire les données depuis azure plus facilement, et les loggers qui seront très importantes afin de poursuivre l'architecture.
Depuis la semaine 3, j'avais commencer déjà à tester les différentes parties sas en python, surtout les filtres et les colonnes, et aussi, j'ai écris un script python qui me permettrait quand les vrais données de tests seront disponibles,  de cherche et voir clairement les colonnes qui sont disponibles pour éviter les erreurs. Et j'ai aussi répcupérer les types de chaque colonnes depuis le serveur sas.

Semaine 6 -7 :
Durant les deux semaines qui ont suivi, j'ai principalement travailler sur la transcription de la version sas du datamart construction en version python. je regardais le code sas et je le traduisais en python en suivant l'architecture médaille. On avait déjà décider de l'architecture de sauvegarde avec min tuteur qui change par rapport à la version sas et plus organiser. En plus de cela nous avons choisir pour la première version du parquet qui était plus adapter aux besoins et gardent les les typages avec efficacité.
Et donc, j'ai fais la transcription la première semaine et juste tester sans vraiment prendre en compte l'architecture de mon code. Maintenant que mon code est fonctionnel, dans la semaine qui a suivi, j'ai fait une première optimisation de mon code en restructurant et le rendant configurable et réutilisable exactement comme un vrai package. Jusque là, je fais mes tests avec de fausses données générées cars les données n'étaient pas encore disponibles, donc j'ai fait des tests avec des données artificielles.
Dans la semaine 6, j'ai fait aussi un point de rencontre avec mon M+2 pour faire le point sur l'avancement de mon travail et pour discuter des différentes problématiques que j'ai pu rencontrer. Et donc, il m'a donner des conseils et son avis sur l'évolution de mon travail qui était positif. Et aussi, grâce à elle, j'ai compris, une notion qui était un peu mal comprise à mon niveau.

semaine 8 - 9 : 
Dès la semaine 8, au mileu de la semaine bien sûr, les données de test sont arrivées et j'ai pu commencer à tester mon code avec des données réelles. Alors j'ai pu d'abord, corriger mon dictionnaire de données pour chaque tables, parceque certaines colonnes n'existent pas dans ce que j'avais mis, car ces colonnes sont créer au bout du fonctionnement du datamart et n'existaient pas déjà dès le début.
Aussi, je me suis rendu compte qu'il y avait une grande différence entre le fonctionnement de sas et python que je n'avais pas pris en compte dans mon code, il s'agit de la version de nulls.
POur  une petite explication, sas considère les données numérique null comme "." et les données textuelle null comme "" ou " ". et donc j'ai du gérer ce cas avec mon code et modifier mon code pour qu'il puisse gérer correctement les nulls et aussi ça permet aussi d'optmiser le code et utiliser les vrais composants de spark.
Aussi, un autre problème était que j'ai remarquer que avec les vraies données, les calculs donnent tous zeros, ce qui n'est pas logique et pas conforme au résultat donnés par sas, même les les filtres et transformations sont corrects. Donc j'ai du chercher tout venait exactement le problème toute la semaine.

semaine 10 - 13 : 
Das cette semaine, j'ai du changer d'approche et restructurer le code, ainsi bien repositionner les éléments et comparés mon code plus minicieusement avec le code sas, et aussi, j'ai pu me rendre compte que j'ai laisser certain filtres que j'ai créer, mais pas intégrer dans le processus final, j'ai du faire une comparaison vraiment ligne par ligne, corriger les erreurs de frappes  que j'ai pu faire pour certains filtres et autre. et maintenant certaines parties sont correctes et fonctionnelles et correspondent aux résultats attendus, mais les calculs étaient toujours nulls, donc j'ai du faire une analyse plus possé des donnés, enlever certain filtres qui étaient configurer en json, directement en pyspark pour éviter les ambiguter, et aussi, je suis aller revoir les données encore une n ieme fois, et c'est là où je me suis rendu compte d'une erreur subtile que je n'avais pas pris en compte car sas les géraient tous de la même manière. Les dates avais des formats différents, certains au formats européens et autres au formats américains, ... Et donne j'ai du revoir le format dans chaque données et m'assurer que les données étaient bien formaté. et aussi sur  les conseils de mon tuteur j'ai fais des recherches sur les avantages de delta tables que j'ai intégrer au sauvegarde des données.


et avec tous ces changements,le résultat à l'air de correspondre au études de KPI qu'on faisait en comparaison avec sas.



Info :
Dans le même temps, j'ai fais d'autres rapport et mise à jour des documentations et tout le reste.

Je fais le point avec mon tuteur et mon M+1 sur mon travail chaque deux semaines.

La vie en équipe, est très conviviale et agréable, avec des collègues très sympathiques et des taches qui sont très intéressantes. L'équipe orginise très souvent des évènement pour se rapprocher et se connecter. comme les café du matin et les petits dejeuner de pole.

Aussi, des réunions très importants s'oganisent qui  permettent de discuter des différents projets et des différents sujets, mais aussi de rester informé de tout ce qui se passe au niveau du groupe, aussi voir ce que fait les autres poles. (comme les point point ADC et les virtual coffee)

Durant Noel, tout était agréable et le pole a organisé des évènements qui étaient très bien et on ne fait pas que travailler, quoi il y a de l'humain.

J'ai aussi participer aux évènements et réunions de nouveaux entrants qui m'ont permit de plus comprendre l'hiérachi et comment fontionne la grande majorité des autres pole, mais aussi rencontrer les alternants et autres.