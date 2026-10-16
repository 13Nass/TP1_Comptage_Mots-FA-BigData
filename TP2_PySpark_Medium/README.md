# TP2 — Analyse des accidents de la route avec PySpark

## Présentation

Dans le cadre de mon cours de Big Data, j'ai réalisé ce TP afin de découvrir différentes méthodes de traitement de données avec Apache Spark et PySpark.

Le travail consiste à reproduire une étude sur les accidents de la route en Australie-Méridionale entre 2012 et 2019, publiée par Rakesh Nain sur Medium.

L'objectif était de reprendre les analyses proposées, de comprendre leur fonctionnement et de comparer les trois approches PySpark : **RDD, DataFrame et Spark SQL**.

J'ai également essayé d'aller plus loin que l'étude originale en apportant quelques améliorations, notamment sur le nettoyage des données, les jointures et les performances.

## Technologies utilisées

- Python
- PySpark 3.5.8
- Apache Spark
- Spark SQL
- Google Colab
- Pandas, Matplotlib et NumPy
- Java 17

J'ai réalisé les traitements directement sur Google Colab, avec Spark configuré en mode local.

## Jeux de données

Les données proviennent du dépôt GitHub associé à l'article original.

J'ai utilisé deux ensembles de données :

- **Crash** : informations sur les accidents, leur localisation, leur gravité et le nombre de victimes.
- **Units** : informations sur les véhicules, leurs occupants et les permis de conduire.

Les fichiers couvrent huit années, de 2012 à 2019.

| Jeu de données | Enregistrements |
|---|---:|
| Crash | 127 672 |
| Units | 272 591 |

Le lien entre les deux jeux de données est réalisé avec la colonne `REPORT_ID`.

## Travail réalisé

J'ai commencé par installer PySpark et charger les différents fichiers CSV dans des DataFrames.

J'ai ensuite reproduit les principales analyses de l'article :

- Partitionnement des données avec les RDD.
- Calcul de l'âge moyen selon le sexe.
- Recherche des véhicules les plus anciens et les plus récents.
- Recherche des accidents ayant causé le plus de victimes.
- Analyse des décès selon le type d'accident.
- Analyse des accidents impliquant des personnes sans permis.
- Jointures entre les données Crash et Units.
- Comparaison des traitements avec RDD, DataFrame et Spark SQL.
- Analyse de la gravité des accidents selon les indicateurs d'alcool et de drogues.

Pour chaque partie, j'ai essayé de vérifier les résultats et de comprendre les opérations effectuées par Spark.

## Améliorations apportées

En reproduisant l'étude, j'ai remarqué certaines limites dans les traitements proposés. J'ai donc apporté plusieurs modifications en conservant les versions originales pour pouvoir comparer les résultats.

### 1. Gestion des valeurs manquantes

Lors du partitionnement des véhicules, les immatriculations inconnues étaient regroupées avec celles des autres États.

J'ai séparé les données en trois catégories : immatriculations SA, autres immatriculations renseignées et valeurs inconnues.

Cela m'a permis de distinguer les 47 463 enregistrements sans information exploitable sur l'État d'immatriculation.

J'ai également nettoyé les valeurs d'âge avant de calculer les moyennes.

### 2. Correction des classements

Pour le classement des accidents ayant fait le plus de victimes, j'ai ajouté des critères de tri afin de mieux gérer les égalités.

J'ai notamment identifié 11 accidents atteignant le seuil du top 10, alors que la requête initiale n'en affichait que 10.

J'ai donc conservé l'ensemble des ex æquo.

### 3. Correction du double comptage après jointure

C'est l'une des corrections les plus importantes que j'ai réalisées.

En analysant les accidents impliquant au moins une unité associée à la catégorie `Unlicenced`, j'ai constaté qu'un même accident pouvait apparaître plusieurs fois après la jointure.

Cela entraînait une répétition de son nombre de victimes.

J'ai utilisé une jointure `left_semi` pour conserver chaque accident une seule fois.

| Résultat | Total |
|---|---:|
| Avant correction | 2 072 |
| Après correction | 2 053 |
| Surcomptage | 19 |

Cette modification a permis de corriger le résultat de l'analyse.

J'ai ensuite reproduit le calcul avec les RDD et Spark SQL, puis vérifié que les trois méthodes obtenaient les mêmes résultats.

### 4. Analyse de la gravité des accidents

J'ai complété l'analyse du nombre de décès par type d'accident avec la proportion d'accidents mortels.

J'ai également créé quatre catégories distinctes pour étudier les accidents selon les indicateurs DUI et drogues, sans compter plusieurs fois les mêmes accidents.

Cette analyse montre des différences importantes entre les groupes.

Il faut cependant préciser que les valeurs `NULL` ne prouvent pas l'absence d'alcool ou de drogues. Les résultats permettent d'observer des associations dans les données, mais pas de démontrer une relation de cause à effet.

### 5. Vérification des résultats

Pour comparer les trois approches PySpark, je ne me suis pas limité au nombre de lignes obtenues.

J'ai utilisé `exceptAll()` pour vérifier que les résultats étaient réellement identiques, y compris lorsque des doublons étaient présents.

Les comparaisons n'ont révélé aucune différence entre les résultats des RDD, des DataFrames et de Spark SQL pour les requêtes vérifiées.

## Comparaison des performances

J'ai mesuré les temps d'exécution des trois approches avec deux opérations : `count()` et `collect()`.

Chaque test a été répété sept fois et j'ai retenu la médiane des mesures.

| Méthode | `count()` | `collect()` |
|---|---:|---:|
| RDD | 2,3710 s | 2,5140 s |
| DataFrame | 1,6025 s | 1,0608 s |
| Spark SQL | 1,9247 s | 1,0238 s |

Dans mes tests, les DataFrames et Spark SQL ont obtenu des performances assez proches, tandis que les RDD étaient généralement plus lents.

J'ai également étudié les plans d'exécution avec `explain()` et les dépendances RDD avec `toDebugString()`.

Cela m'a permis de retrouver certaines optimisations de Spark, notamment la sélection des colonnes nécessaires et l'utilisation de `BroadcastHashJoin`.

Les mesures ont été réalisées sur Google Colab et peuvent varier selon les ressources disponibles. Elles ne constituent donc pas un classement général des performances de ces trois approches.

## Exécution du notebook

Le projet est disponible dans le fichier `TP2_PySpark_Medium.ipynb`.

Pour l'exécuter :

1. Ouvrir le notebook avec Google Colab.
2. Lancer les cellules d'installation de Java 17 et PySpark.
3. Exécuter les cellules dans l'ordre, depuis le début.
4. Consulter les résultats, les graphiques et les explications.

Les fichiers CSV sont téléchargés depuis le dépôt GitHub de l'étude originale. Il n'est donc pas nécessaire de les ajouter manuellement dans Colab.

Le notebook utilise PySpark 3.5.8 et Java 17.

## Conclusion

Ce TP m'a permis de mieux comprendre le fonctionnement de Spark et les différences entre les RDD, les DataFrames et Spark SQL.

J'ai pu constater que plusieurs méthodes pouvaient produire les mêmes résultats tout en utilisant des mécanismes d'exécution différents.

Ce que je retiens surtout, c'est l'importance de vérifier les données et les résultats, notamment lorsqu'on utilise des jointures ou des agrégations.

Les améliorations que j'ai apportées ne concernent pas uniquement les performances. Certaines permettent surtout d'éviter des erreurs de comptage ou de rendre les statistiques plus faciles à interpréter.

## Sources et crédits

Ce projet est basé sur l'étude de **Rakesh Nain** :

- [Article Medium — Big Data Analysis of Road Crash Data using PySpark](https://medium.com/rakesh-nain/big-data-analysis-of-road-crash-data-using-pyspark-with-pyspark-tutorial-c78ff2c35588)
- [Dépôt GitHub original et jeux de données](https://github.com/RakeshNain/Big-data-Analysis-of-Road-Crash-Data-using-PySpark-with-PySpark-Tutorial)
- [Documentation officielle Apache Spark](https://spark.apache.org/docs/3.5.8/)

Les données et l'étude initiale ne sont pas de moi. Mon travail porte sur leur reproduction, leur analyse et les améliorations apportées aux traitements dans le cadre de ce TP universitaire.