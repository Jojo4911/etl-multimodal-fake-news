"""Chargement du CSV transforme vers PostgreSQL.

Écriture idempotente : PRIMARY KEY sur id, ON CONFLICT DO UPDATE.
Deux executions consecutives ne produisent ni erreur ni doublon.
"""

import csv
import logging
import os

import psycopg2

logger = logging.getLogger(__name__)

COLONNES = [
    "id",
    "source",
    "article_url",
    "title",
    "text_clean",
    "published_at",
    "image_path",
    "image_valid",
    "fetched_at",
]

DDL_SCHEMA = "CREATE SCHEMA IF NOT EXISTS {schema};"

DDL_TABLE = """
CREATE TABLE IF NOT EXISTS {schema}.publications (
    id            TEXT PRIMARY KEY,
    source        TEXT NOT NULL,
    article_url   TEXT NOT NULL,
    title         TEXT,
    text_clean    TEXT,
    published_at  TIMESTAMPTZ,
    image_path    TEXT,
    image_valid   BOOLEAN NOT NULL DEFAULT FALSE,
    fetched_at    TIMESTAMPTZ NOT NULL
);
"""

UPSERT = """
INSERT INTO {schema}.publications (
    id, source, article_url, title, text_clean,
    published_at, image_path, image_valid, fetched_at
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (id) DO UPDATE SET
    title        = EXCLUDED.title,
    text_clean   = EXCLUDED.text_clean,
    published_at = EXCLUDED.published_at,
    image_path   = EXCLUDED.image_path,
    image_valid  = EXCLUDED.image_valid,
    fetched_at   = EXCLUDED.fetched_at;
"""


def config_depuis_environnement():
    """Lit les paramètres de connexion injectés par Docker Compose."""
    return {
        "host": os.environ["CHECKIT_PG_HOST"],
        "port": int(os.environ["CHECKIT_PG_PORT"]),
        "user": os.environ["CHECKIT_PG_USER"],
        "password": os.environ["CHECKIT_PG_PASSWORD"],
        "dbname": os.environ["CHECKIT_PG_DB"],
    }


def lit_csv(chemin_csv):
    """Lit le CSV transforme et renvoie une liste de tuples prêts a insérer."""
    lignes = []
    with open(chemin_csv, newline="", encoding="utf-8") as f:
        lecteur = csv.DictReader(f)
        manquantes = set(COLONNES) - set(lecteur.fieldnames or [])
        if manquantes:
            raise ValueError(f"Colonnes absentes du CSV : {sorted(manquantes)}")
        for ligne in lecteur:
            lignes.append(tuple(normalise(col, ligne[col]) for col in COLONNES))
    logger.info("CSV lu : %s lignes depuis %s", len(lignes), chemin_csv)
    return lignes


def normalise(colonne, valeur):
    """Convertit les valeurs textuelles du CSV vers les types attendus."""
    if valeur == "":
        return None
    if colonne == "image_valid":
        return str(valeur).strip().lower() in {"true", "1", "yes"}
    return valeur


def compte_lignes(cur, schema):
    cur.execute(f"SELECT COUNT(*) FROM {schema}.publications;")
    return cur.fetchone()[0]


def run(chemin_csv, schema=None):
    """Charge le CSV vers Postgres. Renvoie un dict de métriques."""
    schema = schema or os.environ.get("CHECKIT_PG_SCHEMA", "checkit")
    lignes = lit_csv(chemin_csv)

    with psycopg2.connect(**config_depuis_environnement()) as conn:
        with conn.cursor() as cur:
            cur.execute(DDL_SCHEMA.format(schema=schema))
            cur.execute(DDL_TABLE.format(schema=schema))
            avant = compte_lignes(cur, schema)
            cur.executemany(UPSERT.format(schema=schema), lignes)
            apres = compte_lignes(cur, schema)
        conn.commit()

    metriques = {
        "lignes_lues": len(lignes),
        "lignes_avant": avant,
        "lignes_apres": apres,
        "lignes_ajoutees": apres - avant,
        "lignes_mises_a_jour": len(lignes) - (apres - avant),
    }
    logger.info(
        "Chargement termine : %s lues, base %s -> %s, %s ajoutees, %s mises a jour",
        metriques["lignes_lues"],
        metriques["lignes_avant"],
        metriques["lignes_apres"],
        metriques["lignes_ajoutees"],
        metriques["lignes_mises_a_jour"],
    )
    return metriques