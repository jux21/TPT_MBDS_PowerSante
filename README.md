# README #

Moteur de recommandation écrit en Spark Scala avec mongoDB (développement avec Apache Zeppelin en Spark Scala puis projet Maven Spark Java/Scala).


Librairies utilisées :
Python : missingno, pandas, sklearn, matplotlib, numpy,
Spark Scala : MLLib, SparkML, Spark SQL

Lecture des données depuis CSV pour la phase de développement et depuis mongoDB pour la production.

Données : fichiers CSV de parcours client/visiteur (qui a vu quoi et quand), de produits (id et catégorie des produits), de client (infos client) et de visiteurs (infos visiteur)



Prédiction des prochains produits ou catégories de produits susceptible d'être vus par l'utilisateur.

1)    Spécification des différences entre un parcours client et un parcours utilisateur -> tranche d’âge et ville renseignée à l’inscription contre ville géocodée depuis IP
2)    Modélisation d’un parcours utilisateurs (X derniers objets vu + leurs catégories + le timestamp des vues + jointure des infos client/visiteurs)
3)    Feature engineering sur les timestamp des vues pour extraire l'heure, le jour de la semaine, le jour du mois, le mois, le jour de l'année, la saison, la dizaine de minutes etc ...
4)    Gestion des données manquantes catégoriques et numériques
5)    Clusterisation K-means des parcours utilisateurs -> pour choisir le nombre de cluster optimal utilisation de la méthode du elbow (K vs variance expliquée)
6)    Catégorisation + vectorisation + Kmeans : ajustement du nombre de K en fonction de l’homogénéité de la distribution des classes
7)    Apprentissage des classes par un RandomForestClassifier
8)    Métrique de succès de l’apprentissage -> Matrice de confusion + accuracy des prédictions. On obtient entre 70 et 82% d’accuracy
9)    Prédiction -> un web service va passer en paramètre au modèle les X derniers produits vus d'un utilisateur + ses catégories + ses timestamp + son type (visiteur/client) -> On relance toute la procédure de feature engineering, jointure, vectorisation, catégorisation et on appelle la méthode predict() du modèle
10)    La prédiction va donner une classe pour cet utilisateur -> le web service retourne les top items et top catégories de son cluster (groupe homogène d’utilisateurs du site web avec les mêmes caractéristiques que lui). Ces objets et catégories seront recommandés dans son navigateur Web
11)    Les utilisateurs qui se retrouvent dans un cluster avec une seule instance ou qui n'ont pas un historique assez fourni ( < 3 produits) pour déclencher une prédiction se voient proposer un top item/catégorie de tout le site web


Pistes d’amélioration du livrable :
Améliorer le top item/catégorie des utilisateurs sans historique ou sans cluster assez fournis en faisant un top item/catégorie de sa géographie (information disponible pour les visiteurs et les clients)
Proposer également un collaborative filtering (Alice aime item 1 et 2, Bob item 2 et 3 -> je recommande 3 à Alice et 1 à Bob).


Pistes d'amélioration autres :
Tester d'autres classifieurs (Gradient Boosting, SVM, Neural Net, Naive Bayes).
Disposer de plus de données (3 colonnes tag par produit car ici hash hexadécimaux anonymisés impossible à rapprocher entre eux) et sur une plus longue période (pour le feature engineering sur timestamp qui extrait l’année et le mois alors qu’il n’y a ici que 24h de données).
Disposer des achats utilisateurs.
Disposer des notes données par les utilisateurs à chaque achat.


