import os
import pandas as pd
from datetime import datetime

# Répertoires
INPUT_DIR = "data/raw/history"
OUTPUT_DIR = "data/star_schema"
os.makedirs(OUTPUT_DIR, exist_ok=True)       # Crée le dossier si besoin

# Lecture de tous les fichiers météo de ville avec traitement
def charger_toutes_villes():

    all_files = [f for f in os.listdir(INPUT_DIR) if f.endswith(".csv")]
    dfs = []
    for f in all_files:
        path = os.path.join(INPUT_DIR, f)
        df = pd.read_csv(path)

        # Remplacer les NaN par 0 AVANT traitement
        df.fillna(0, inplace=True)

        # Limiter les décimales à 2
        colonnes_numeriques = [
            "temperature_2m_min", "temperature_2m_max",
            "precipitation_sum", "humidity", "pressure", "wind_speed"
        ]
        for col in colonnes_numeriques:
            if col in df.columns:
                df[col] = df[col].round(2)

        df["ville"] = f.replace("meteo_", "").replace(".csv", "").replace("_", " ").title()
        dfs.append(df)
    return pd.concat(dfs, ignore_index=True)


# Dimension date
def construire_dim_date(df):
    df["time"] = pd.to_datetime(df["time"])
    dim_date = df[["time"]].drop_duplicates().copy()
    dim_date["date_id"] = dim_date["time"].rank(method='dense').astype(int)
    dim_date["année"] = dim_date["time"].dt.year
    dim_date["mois"] = dim_date["time"].dt.month
    dim_date["jour"] = dim_date["time"].dt.day
    return dim_date

# Dimension ville (avec latitude, longitude)
def construire_dim_ville(df):
    dim_ville = df[["ville", "latitude", "longitude"]].drop_duplicates().copy()
    dim_ville["ville_id"] = range(1, len(dim_ville) + 1)
    dim_ville.rename(columns={"ville": "nom"}, inplace=True)
    return dim_ville

# Dimension source
def construire_dim_source():
    return pd.DataFrame([
        {"source_id": 1, "nom_source": "open-meteo", "description": "API historique"},
        {"source_id": 2, "nom_source": "openweather", "description": "API temps réel"}
    ])

# Table des faits
def construire_fait(df, dim_date, dim_ville, dim_source):
    df = df.merge(dim_date[["time", "date_id"]], on="time")
    df = df.merge(dim_ville[["nom", "ville_id"]], left_on="ville", right_on="nom")
    df = df.merge(dim_source[["nom_source", "source_id"]], left_on="source", right_on="nom_source")

    df["temp_mean"] = (df["temperature_2m_min"] + df["temperature_2m_max"]) / 2
    df["temp_mean"] = df["temp_mean"].round(2)

    df["rainy_day"] = (df["precipitation_sum"] > 0).astype(int)

    fact = df[[ 
        "date_id", "ville_id", "source_id",
        "temperature_2m_min", "temperature_2m_max", "temp_mean",
        "precipitation_sum", "humidity", "pressure", "wind_speed",
        "rainy_day"
    ]].copy()
    fact["id"] = range(1, len(fact) + 1)
    return fact
