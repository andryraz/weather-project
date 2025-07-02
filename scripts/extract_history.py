import pandas as pd
import requests
from datetime import datetime
import os
import logging

# Configuration du logger:  écrit les messages INFO / WARNING / ERROR
logging.basicConfig(level=logging.INFO)

def get_historique_openmeteo(ville, lat, lon, start, end) -> bool:
    """
    Extrait les données historique météo (quotidien + horaire) d'une ville
    depuis l'API OpenMeteo. Puis les sauvegardes dans un fichier csv pour 
    chaque ville.

    Il est a noter que certaines données provenant d'OpenMeteo sont horaires 
    mais pas journalières donc on est obligé de faire des calcules moyennes 
    horaires pour avoir des données journalières.
    
    Args:
        ville : Nom de la ville à interroger
        lat : latitude de la ville à interroger
        lon : longitude de la ville à interroger
        start : Date de debut des donnees a recuperer au format 'YYYY-MM-DD' 
        end : Date de fin des donnees a recuperer au format 'YYYY-MM-DD'
        
    Returns:
        bool: True si l'extraction réussit, False sinon
        
    Exemple:
        >>> get_historique_openmeteo("Londres", "51.50", "0.12", "2024-06-01", "2025-06-01")
    """
    try:
        url = "https://archive-api.open-meteo.com/v1/archive"
        daily_params = {
            "latitude": lat,
            "longitude": lon,
            "start_date": start,
            "end_date": end,
            "daily": "temperature_2m_min,temperature_2m_max,precipitation_sum",
            "timezone": "UTC"
        }

        daily_response = requests.get(url, params=daily_params, timeout=10)
        daily_response.raise_for_status()  # Lève une exception si code != 200

        data_daily = daily_response.json().get("daily")
        if data_daily is None:
            raise KeyError("Clé 'daily' absente dans la réponse")
        df_daily = pd.DataFrame(data_daily)
        df_daily["time"] = pd.to_datetime(df_daily["time"]).dt.date

        #Requête hourly (humid, pression, vent)
        hourly_params = {
            "latitude": lat, 
            "longitude": lon,
            "start_date": start, 
            "end_date": end,
            "hourly": "relative_humidity_2m,pressure_msl,windspeed_10m",
            "timezone": "UTC"
        }

        hourly_response = requests.get(url, params=hourly_params, timeout=10)
        hourly_response.raise_for_status() 
        data_hourly = hourly_response.json().get("hourly")


        if data_hourly is None:
            raise KeyError("Clé 'hourly' absente dans la réponse")

        df_hourly = pd.DataFrame(data_hourly)
        df_hourly["time"] = pd.to_datetime(df_hourly["time"])
        df_hourly["date"] = df_hourly["time"].dt.date

        # Moyenne horaire pour journalière
        df_hourly_mean = (df_hourly
            .groupby("date", as_index=False)[
                ["relative_humidity_2m", "pressure_msl", "windspeed_10m"]
            ].mean()
            .rename(columns={
                "date": "time",
                "relative_humidity_2m": "humidity",
                "pressure_msl": "pressure",
                "windspeed_10m": "wind_speed"
            })
        )

        # Fusion daily + hourly_mean      
        df = df_daily.merge(df_hourly_mean, on="time", how="left")
        df["ville"] = ville
        df["source"] = "open-meteo"
        df["latitude"] = lat
        df["longitude"] = lon
        df["time"] = pd.to_datetime(df["time"])  

        # Créer le dossier de sortie s’il n’existe pas
        os.makedirs("data/raw/history", exist_ok=True)

        # Sauvegarde dans un fichier CSV spécifique à la ville
        safe_ville = ville.replace(" ", "_").lower()  
        chemin = f"data/raw/history/meteo_{safe_ville}.csv"
        df.to_csv(chemin, index=False)

        logging.info(f"Données enregistrées pour {ville} dans {chemin}")
        return df


    except requests.exceptions.RequestException as e:
        logging.error(f"Erreur réseau/API pour {ville}: {str(e)}")
    except KeyError as e:
        logging.error(f"Champ manquant dans la réponse pour {ville}: {str(e)}")
    except Exception as e:
        logging.error(f"Erreur inattendue pour {ville}: {str(e)}")

    return None  
