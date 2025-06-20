import os
import pandas as pd
from datetime import datetime

# Répertoires
INPUT_DIR = "data/raw/history"
OUTPUT_DIR = "data/star_schema"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Lecture de tous les fichiers météo de ville
def charger_toutes_villes():
    all_files = [f for f in os.listdir(INPUT_DIR) if f.endswith(".csv")]
    dfs = []
    for f in all_files:
        path = os.path.join(INPUT_DIR, f)
        df = pd.read_csv(path)
        df["ville"] = f.replace("meteo_", "").replace(".csv", "").replace("_", " ").title()
        dfs.append(df)
    return pd.concat(dfs, ignore_index=True)

# Dimension date
def construire_dim_date(df):
    df["time"] = pd.to_datetime(df["time"])
    dim_date = df[["time"]].drop_duplicates().copy()
    dim_date["date_id"] = dim_date["time"].rank(method='dense').astype(int)
    dim_date["annee"] = dim_date["time"].dt.year
    dim_date["mois"] = df["time"].dt.month
    dim_date["jour"] = df["time"].dt.day
    return dim_date

# Dimension ville (géolocalisation non connue ici)
def construire_dim_ville(df):
    dim_ville = pd.DataFrame(df["ville"].unique(), columns=["nom"])
    dim_ville["ville_id"] = dim_ville.index + 1
    return dim_ville

# Dimension source
def construire_dim_source():
    return pd.DataFrame([
        {"source_id": 1, "nom_source": "open-meteo", "description": "API historique"},
        {"source_id": 2, "nom_source": "openweather", "description": "API temps reel"}
    ])

# Table des faits
def construire_fait(df, dim_date, dim_ville, dim_source):
    df = df.merge(dim_date[["time", "date_id"]], on="time")
    df = df.merge(dim_ville[["nom", "ville_id"]], left_on="ville", right_on="nom")
    df = df.merge(dim_source[["nom_source", "source_id"]], left_on="source", right_on="nom_source")

    df["temp_mean"] = (df["temperature_2m_min"] + df["temperature_2m_max"]) / 2

    fact = df[[
        "date_id", "ville_id", "source_id",
        "temperature_2m_min", "temperature_2m_max", "temp_mean",
        "precipitation_sum"
    ]].copy()
    fact["id"] = range(1, len(fact) + 1)
    return fact
