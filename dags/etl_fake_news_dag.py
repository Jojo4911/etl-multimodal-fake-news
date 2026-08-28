"""DAG ETL : extraction, transformation et chargement de publications RFI.

Projet 12, CheckIt.AI. Livrable 5.
Trois tâches separées, une par étape du pipeline. Les tâches communiquent
par chemins de fichiers via XCom, jamais par charge utile.
"""

from __future__ import annotations

import csv
import logging
from datetime import datetime
from pathlib import Path

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

import extract_rss
import transform
import load

logger = logging.getLogger(__name__)

# Chemins absolus dans le conteneur. Le répertoire courant d'un worker
# Airflow n'est pas garanti : on ne s'appuie sur aucun chemin relatif.
DATA_DIR = Path("/opt/airflow/data")
RAW_DIR = DATA_DIR / "raw"
IMAGES_DIR = DATA_DIR / "images"
PROCESSED_DIR = DATA_DIR / "processed"

def tache_extract() -> str:
    """Collecte le flux RFI et retourne le chemin du fichier JSON Lines."""
    chemin = extract_rss.run(raw_dir=RAW_DIR, images_dir=IMAGES_DIR)
    if chemin is None:
        raise ValueError("Extraction echouée : aucun fichier brut produit")
    return str(chemin)


def tache_transform(ti) -> str:
    """Lit le fichier brut produit en amont et retourne le chemin du CSV."""
    chemin_brut = ti.xcom_pull(task_ids="extract")
    return transform.run(input_path=chemin_brut, out_dir=str(PROCESSED_DIR))


def tache_load(ti):
    chemin_csv = ti.xcom_pull(task_ids="transform")
    if not chemin_csv:
        raise ValueError("Aucun chemin CSV reçu de la tâche transform")
    metriques = load.run(chemin_csv)
    return metriques


with DAG(
    dag_id="etl_fake_news",
    schedule="@daily",
    start_date=datetime(2026, 8, 20),
    catchup=False,
) as dag:

    extract_step = PythonOperator(
        task_id="extract",
        python_callable=tache_extract,
    )

    transform_step = PythonOperator(
        task_id="transform",
        python_callable=tache_transform,
    )

    load_step = PythonOperator(
        task_id="load",
        python_callable=tache_load,
    )

    extract_step >> transform_step >> load_step