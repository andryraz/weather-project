from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging

# Import fonctions
from comparing_weather.scripts.extract_history import get_historique_openmeteo
from comparing_weather.scripts.extract_real import get_temps_reel_openweather
from comparing_weather.scripts.transform import (
    construire_dim_date,
    construire_dim_ville,
    construire_dim_source,
    construire_fait,
    charger_toutes_villes
)

# Paramètres du DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 19),
}

with DAG(
    'weather_pipeline_dag',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    max_active_runs=1,  
) as dag:

    villes = {
        "Paris": (48.85, 2.35),
        "Antananarivo": (-18.88, 47.51),
        "New York": (40.71, -74.00)
    }

    api_key = "43b6dc83753970c6271c339c12d3bb6e"

    def etape_1_historique():
        for ville, (lat, lon) in villes.items():
            get_historique_openmeteo(ville, lat, lon, start="2023-01-01", end="2025-06-18")

    def etape_2_temps_reel():
        for ville, (lat, lon) in villes.items():
            get_temps_reel_openweather(ville, lat, lon, api_key)

    def etape_3_star_schema():
        df = charger_toutes_villes()

        dim_date = construire_dim_date(df)
        dim_date.to_csv("data/star_schema/dim_date.csv", index=False)

        dim_ville = construire_dim_ville(df)
        dim_ville.to_csv("data/star_schema/dim_ville.csv", index=False)

        dim_source = construire_dim_source()
        dim_source.to_csv("data/star_schema/dim_source.csv", index=False)

        fact_weather = construire_fait(df, dim_date, dim_ville, dim_source)
        fact_weather.to_csv("data/star_schema/fact_weather.csv", index=False)

        logging.info("Modèle en étoile mis à jour.")

    t1_historique = PythonOperator(
        task_id='recuperer_historique_openmeteo',
        python_callable=etape_1_historique
    )

    t2_temps_reel = PythonOperator(
        task_id='recuperer_temps_reel_openweather',
        python_callable=etape_2_temps_reel
    )

    t3_star_schema = PythonOperator(
        task_id='generer_schema_etoile',
        python_callable=etape_3_star_schema
    )

    # Dépendances
    t1_historique >> t2_temps_reel >> t3_star_schema
