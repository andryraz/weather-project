# 🧱 Schéma en étoile

Les données sont structurées pour une visualisation efficace :

## Dimensions

- `dim_date` : date_id, time, année, mois, jour
- `dim_ville` : ville_id, nom, latitude, longitude
- `dim_source` : source_id, nom_source, description

## Faits

- `fact_weather` : mesures météo, jours de pluie, moyennes
