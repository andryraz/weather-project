        Documentation du projet : Comparaison météorologique
Bienvenue dans la documentation du projet de comparaison météorologique.

Ce projet a pour objectif d'étudier l'évolution climatique de plusieurs grandes villes du monde afin de les comparer selon :

-leur stabilité,

-leur niveau de chaleur,

-leur variabilité météorologique.



        Sources de données
Le projet collecte, transforme, analyse et visualise(via power bi) des données météorologiques historiques (5+ ans), combinées à des données en temps réel pour chaque ville.

1. Open-Meteo (Historique)
URL : https://archive-api.open-meteo.com/v1/archive

Données disponibles : températures journalières, précipitations, humidité, pression, vitesse du vent

2. OpenWeather (Temps réel)
URL : https://api.openweathermap.org/data/2.5/weather

Authentification : nécessite une clé API

Données disponibles : température, humidité, pluie, pression, vent

Les historiques de chaque ville sont stockés dans des fichiers CSV dans le dossier data/raw/history, puis fusionnés avec les données en temps réel d’OpenWeather.



        Orchestration avec Airflow
L’orchestration du pipeline est réalisée à l’aide de Apache Airflow, avec du code écrit en Python.

Ordre des tâches dans le pipeline
*recuperer_historique_openmeteo
→ Télécharge les données historiques depuis Open-Meteo

*recuperer_temps_reel_openweather
→ Ajoute les données en temps réel depuis OpenWeather

*generer_schema_etoile
→ Construit et exporte le modèle en étoile pour l’analyse

Chaque tâche appelle un script Python depuis le dossier scripts/.
Le fichier principal du DAG est weather_etl.py, situé dans le répertoire dags/.

Variable requise
API_KEY : clé API OpenWeather à définir dans les Variables Airflow.



        Modélisation des données : modèle en étoile
Le pipeline produit un modèle en étoile pour organiser les données météo.

Tables de dimensions
dim_date : date_id, time, année, mois, jour

dim_ville : ville_id, nom, latitude, longitude

dim_source : source_id, nom_source, description

Table de faits
fact_weather : contient les mesures météorologiques (température, pluie, humidité, pression, vent), avec les clés date_id, ville_id, source_id
 Ces tables sont exportées dans le dossier data/star_schema/.



        Analyse exploratoire des données (EDA)
Pour mieux comprendre les données, une analyse exploratoire (EDA) a été réalisée.

Le notebook EDA est situé dans le dossier EDA/
Il inclut : statistiques descriptives, corrélations, visualisations temporelles...