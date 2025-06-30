# 🔄 Pipeline ETL

Le pipeline est orchestré par Apache Airflow.

### Ordre des tâches

1. `recuperer_historique_openmeteo`
2. `recuperer_temps_reel_openweather`
3. `generer_schema_etoile`

Chaque étape appelle un script Python situé dans `scripts/`.

Voir le fichier `weather_etl.py` dans `dags/comparing_weather/dags/`
