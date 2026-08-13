# P12 : extraction de données multimodales pour détection de fake news

## Environnement

- Airflow 3.3.1 (Docker Compose, quickstart officiel)
- PythonOperator : `airflow.providers.standard.operators.python`
- Python local : 3.13, géré par uv

## Démarrage

1. Créer `.env` à la racine avec `AIRFLOW_UID=50000`
2. `docker compose up airflow-init`
3. `docker compose up -d`
4. UI : http://localhost:8080, compte `airflow` / `airflow`

## Convention de nommage des livrables

Archive : `Extrayez_des_donnees_multimodales_de_sites_web_Fernandez_Jonathan.zip`
Fichiers : `Fernandez_Jonathan_<n>_<nom_du_livrable>_082026`

| N° | Livrable                         | Nom de fichier                                           |
|----|----------------------------------|----------------------------------------------------------|
| 1  | Rapport d'exploration de sources | `Fernandez_Jonathan_1_rapport_exploration_082026.md`     |
| 2  | Scripts d'extraction automatisée | `Fernandez_Jonathan_2_scripts_extraction_082026.py`      |
| 3  | Pipeline de transformation       | `Fernandez_Jonathan_3_pipeline_transformation_082026.py` |
| 4  | Schéma de données finalisé       | `Fernandez_Jonathan_4_schema_donnees_082026.md`          |
| 5  | Flux ETL Airflow                 | `Fernandez_Jonathan_5_flux_etl_082026.py`                |
| 6  | Tableau de bord KPI              | `Fernandez_Jonathan_6_dashboard_kpi_082026.py`           |
| 7  | Plan de monitoring               | `Fernandez_Jonathan_7_plan_monitoring_082026.md`         |