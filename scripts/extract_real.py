import os
import pandas as pd
import requests
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)

def get_temps_reel_openweather(ville, lat, lon, api_key):
    try:
        url = "https://api.openweathermap.org/data/2.5/weather"
        params = {
            "lat": lat,
            "lon": lon,
            "appid": api_key,
            "units": "metric",
            "lang": "fr"
        }

        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        d = r.json()

        df_now = pd.DataFrame([{
            "time": datetime.utcnow().strftime("%Y-%m-%d"),
            "temperature_2m_min": d["main"]["temp_min"],
            "temperature_2m_max": d["main"]["temp_max"],
            "precipitation_sum": d.get("rain", {}).get("1h", 0),
            "ville": ville,
            "source": "openweather"
        }])

        # Déterminer le chemin du fichier d’historique
        safe_ville = ville.replace(" ", "_").lower()
        chemin_csv = f"data/raw/history/meteo_{safe_ville}.csv"
        os.makedirs("data/raw/history", exist_ok=True)

        if os.path.exists(chemin_csv):
            df_hist = pd.read_csv(chemin_csv)

            # Vérifie si la date est déjà présente
            if df_now["time"].iloc[0] in df_hist["time"].values:
                logging.info(f"Donnée du {df_now['time'].iloc[0]} déjà présente pour {ville}")
                return df_hist  # Ne rien faire
            else:
                df_combined = pd.concat([df_hist, df_now], ignore_index=True)
        else:
            logging.info(f"Fichier inexistant pour {ville} : création...")
            df_combined = df_now

        df_combined.to_csv(chemin_csv, index=False)
        logging.info(f"Données temps réel ajoutées à {chemin_csv}")
        return df_combined

    except Exception as e:
        logging.error(f"Erreur OpenWeather pour {ville}: {str(e)}")
        return None
