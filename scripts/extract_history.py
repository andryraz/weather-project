import pandas as pd
import requests
from datetime import datetime
import os
import logging

# Configuration du logger
logging.basicConfig(level=logging.INFO)

def get_historique_openmeteo(ville, lat, lon, start, end):
    try:
        url = "https://archive-api.open-meteo.com/v1/archive"
        params = {
            "latitude": lat,
            "longitude": lon,
            "start_date": start,
            "end_date": end,
            "daily": "temperature_2m_min,temperature_2m_max,precipitation_sum",
            "timezone": "Etc/UTC"
        }

        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()  # Lève une exception si code != 200

        data = response.json().get("daily")
        if data is None:
            raise KeyError("Clé 'daily' absente dans la réponse")


        # Créer le dossier de sortie s’il n’existe pas
        os.makedirs("data/raw/history", exist_ok=True)

        # Création du DataFrame
        df = pd.DataFrame(data)
        df["ville"] = ville
        df["source"] = "open-meteo"

        # Sauvegarde dans un fichier CSV spécifique à la ville
        safe_ville = ville.replace(" ", "_").lower()  # nom de fichier propre
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

    return None  # Meilleur que `False` pour cohérence pandas
