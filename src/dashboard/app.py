"""Tableau de bord des KPI du pipeline ETL de CheckIt.AI.

Le tableau de bord lit, il n'écrit rien et ne déclenche aucune execution.
Deux sources, toutes deux en lecture seule :

  - les fichiers de data/raw et data/processed, qui donnent le nombre
    d'entrées recues du flux RSS et le nombre d'entrées retenues après
    transformation, donc le taux de rejet du pipeline ;
  - la base PostgreSQL du Docker Compose d'Airflow, qui porte a la fois la
    table checkit.publications et les metadonnées d'execution des tâches
    du DAG, dans la table task_instance.

Prérequis :
  - stack Airflow demarrée, port Postgres publié sur 127.0.0.1:5433 ;
  - streamlit et psycopg2-binary installés dans l'environnement local.

Lancement depuis la racine du projet :
    uv run streamlit run src/dashboard/app.py
"""

from __future__ import annotations

import os
from pathlib import Path

import altair as alt
import pandas as pd
import psycopg2
import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = ROOT / "data" / "raw"
PROCESSED_DIR = ROOT / "data" / "processed"
ENV_PATH = ROOT / ".env"

# Les executions du pipeline sont regroupées à la minute. Le champ
# fetched_at peut porter un horodatage par entrée plutôt qu'un horodatage
# par lot : la troncature à la minute rassemble correctement les deux cas,
# et deux executions de test restent separées car espacées de plusieurs
# minutes.
GRANULARITE_RUN = "minute"

# Etats terminaux de task_instance. Airflow en connait d'autres, transitoires,
# qui ne doivent pas entrer dans le calcul d'un taux d'echec.
ETATS_ECHEC = ("failed", "upstream_failed")
ETATS_TERMINES = ("success", "failed", "upstream_failed")

LIBELLES_TACHES = {
    "extract": "Extraction du flux RSS",
    "transform": "Transformation et nettoyage",
    "load": "Chargement en base",
}


def libelle_tache(task_id: str) -> str:
    """Traduit un identifiant technique de tache en libellé metier."""
    cle = task_id.lower().replace("tache_", "").replace("task_", "")
    return LIBELLES_TACHES.get(cle, task_id)


def lire_fichier_env(chemin: Path) -> dict[str, str]:
    """Lit un .env simple, sans dependance externe.

    Encodage utf-8-sig : PowerShell 5.1 écrit un BOM en tête de fichier,
    qui autrement se retrouverait collé à la premiere clé.
    """
    valeurs: dict[str, str] = {}
    if not chemin.exists():
        return valeurs
    for ligne in chemin.read_text(encoding="utf-8-sig").splitlines():
        ligne = ligne.strip()
        if not ligne or ligne.startswith("#") or "=" not in ligne:
            continue
        cle, _, valeur = ligne.partition("=")
        valeurs[cle.strip()] = valeur.strip().strip('"').strip("'")
    return valeurs


# Les variables réellement définies dans l'environnement l'emportent sur le
# fichier .env. Hôte et port sont propres au tableau de bord : depuis le
# conteneur Airflow la base répond sur postgres:5432, depuis la machine hôte
# elle répond sur le port publié.
ENV = {**lire_fichier_env(ENV_PATH), **os.environ}

CONNEXION = {
    "host": ENV.get("CHECKIT_DASHBOARD_PG_HOST", "127.0.0.1"),
    "port": int(ENV.get("CHECKIT_DASHBOARD_PG_PORT", "5433")),
    "dbname": ENV.get("CHECKIT_PG_DB", ENV.get("CHECKIT_PG_DATABASE", "airflow")),
    "user": ENV.get("CHECKIT_PG_USER", "airflow"),
    "password": ENV.get("CHECKIT_PG_PASSWORD", "airflow"),
}
SCHEMA = ENV.get("CHECKIT_PG_SCHEMA", "checkit")


@st.cache_data(ttl=60, show_spinner=False)
def interroge(sql: str, params: tuple | None = None) -> pd.DataFrame:
    """Execute une requête en lecture et renvoie un DataFrame."""
    with psycopg2.connect(**CONNEXION) as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        colonnes = [d[0] for d in cur.description]
        lignes = cur.fetchall()
    return pd.DataFrame(lignes, columns=colonnes)


@st.cache_data(ttl=60, show_spinner=False)
def volumes_fichiers() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Compte les entrées brutes et les entrées retenues, fichier par fichier."""
    brut = [
        {"fichier": p.name, "entrees": sum(1 for l in p.open(encoding="utf-8") if l.strip())}
        for p in sorted(RAW_DIR.glob("*.jsonl"))
    ]
    retenu = [
        {"fichier": p.name, "entrees": len(pd.read_csv(p))}
        for p in sorted(PROCESSED_DIR.glob("*.csv"))
    ]
    return pd.DataFrame(brut), pd.DataFrame(retenu)


st.set_page_config(page_title="CheckIt.AI : pipeline ETL", layout="wide")
st.title("CheckIt.AI : santé du pipeline d'acquisition")
st.caption(
    "Publications d'actualité extraites du flux RSS de RFI, nettoyées, "
    "puis chargées en base pour alimenter le détecteur de fake news."
)

# --- Connexion et garde-fou -------------------------------------------
try:
    taches = interroge(
        """
        SELECT dag_id, run_id, task_id, state, start_date, end_date, duration
        FROM task_instance
        ORDER BY start_date
        """
    )
except Exception as erreur:  # noqa: BLE001
    st.error(f"Base de données injoignable sur {CONNEXION['host']}:{CONNEXION['port']}.")
    st.info(
        "Vérifier que la stack tourne et que le port est publié :\n\n"
        "1. dans docker-compose.yaml, service postgres, ajouter "
        '`ports: ["127.0.0.1:5433:5432"]`\n'
        "2. `docker compose down` puis `docker compose up -d`"
    )
    st.exception(erreur)
    st.stop()

dags = sorted(taches["dag_id"].dropna().unique().tolist())
if not dags:
    st.warning("Aucune exécution de tâche enregistrée dans Airflow.")
    st.stop()

defaut = next((i for i, d in enumerate(dags) if "fake" in d or "etl" in d), 0)
dag_choisi = st.selectbox("Pipeline observé", dags, index=defaut)
taches = taches[taches["dag_id"] == dag_choisi].copy()
taches["etape"] = taches["task_id"].map(libelle_tache)

publications = interroge(
    f"""
    SELECT date_trunc('{GRANULARITE_RUN}', fetched_at) AS execution,
           count(*)                                    AS publications,
           count(*) FILTER (WHERE image_valid::boolean) AS avec_image
    FROM {SCHEMA}.publications
    GROUP BY 1
    ORDER BY 1
    """
)
if not publications.empty:
    publications["avec_image"] = publications["avec_image"].astype(int)
    publications["publications"] = publications["publications"].astype(int)
    publications["sans_image"] = publications["publications"] - publications["avec_image"]
    publications["libelle"] = pd.to_datetime(publications["execution"]).dt.strftime("%d/%m %H:%M")

brut, retenu = volumes_fichiers()

# --- Indicateurs de tête ----------------------------------------------
# Les cinq taux sont calculés sur l'integralité des données disponibles,
# jamais sur la sélection filtrée plus bas. Un taux d'echec calculé apres
# avoir masqué les échecs afficherait zéro, ce qui serait un chiffre faux.
total_brut = int(brut["entrees"].sum()) if not brut.empty else 0
total_retenu = int(retenu["entrees"].sum()) if not retenu.empty else 0
taux_retenu = total_retenu / total_brut if total_brut else 0.0

total_publications = int(publications["publications"].sum()) if not publications.empty else 0
total_avec_image = int(publications["avec_image"].sum()) if not publications.empty else 0
taux_image = total_avec_image / total_publications if total_publications else 0.0

terminees = taches[taches["state"].isin(ETATS_TERMINES)]
taux_echec = (
    terminees["state"].isin(ETATS_ECHEC).mean() if not terminees.empty else 0.0
)

durees = taches.dropna(subset=["duration"]).copy()
durees["duration"] = durees["duration"].astype(float)
duree_moyenne_run = (
    durees.groupby("run_id")["duration"].sum().mean() if not durees.empty else 0.0
)

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric(
    "Articles retenus",
    f"{taux_retenu:.0%}",
    help="Part des articles reçus du flux RSS qui passent les contrôles de qualité "
    "et arrivent jusqu'à la base. Le reste est écarté : texte trop court ou image absente.",
)
c2.metric(
    "Articles illustrés",
    f"{taux_image:.0%}",
    help="Part des publications en base dont l'image a été téléchargée et vérifiée. "
    "Ce taux vaut 100 % par construction : le pipeline écarte en amont les articles "
    "sans image exploitable. Leur volume se lit sur la carte « Articles retenus ».",
)
c3.metric(
    "Publications en base",
    f"{total_publications}",
    help="Nombre total de publications disponibles pour l'entraînement.",
)
c4.metric(
    "Étapes en échec",
    f"{taux_echec:.0%}",
    help="Part des étapes du pipeline qui se sont terminées en erreur, sur "
    "l'ensemble des exécutions. Calculé sur la série complète.",
)
c5.metric(
    "Durée d'un passage",
    f"{duree_moyenne_run:.1f} s",
    help="Temps moyen nécessaire au pipeline pour aller du flux RSS à la base.",
)

st.caption(
    f"{total_brut} articles reçus du flux depuis le début, {total_retenu} retenus, "
    f"{total_publications} présents en base. Ces cinq chiffres portent sur la totalité "
    "de l'historique et ne bougent pas avec les filtres ci-dessous."
)

st.divider()

# --- Volume et qualité par execution -----------------------------------
st.subheader("Ce que rapporte chaque passage du pipeline")

if publications.empty:
    st.warning("Aucune publication en base. Déclencher le DAG dans Airflow.")
else:
    nb_max = len(publications)
    nb_affiche = st.slider(
        "Nombre d'exécutions affichées",
        min_value=1,
        max_value=nb_max,
        value=nb_max,
        help="Les exécutions les plus récentes sont conservées en priorité.",
    )
    vue = publications.tail(nb_affiche)

    empile = vue.melt(
        id_vars=["libelle"],
        value_vars=["avec_image", "sans_image"],
        var_name="qualite",
        value_name="nombre",
    )
    empile["qualite"] = empile["qualite"].map(
        {"avec_image": "Avec image exploitable", "sans_image": "Sans image exploitable"}
    )

    graphique_volume = (
        alt.Chart(empile)
        .mark_bar()
        .encode(
            x=alt.X("libelle:N", title="Exécution du pipeline", sort=None),
            y=alt.Y("nombre:Q", title="Publications enregistrées"),
            color=alt.Color(
                "qualite:N",
                title="Qualité",
                scale=alt.Scale(range=["#4c78a8", "#e0e0e0"]),
            ),
            tooltip=[
                alt.Tooltip("libelle:N", title="Exécution"),
                alt.Tooltip("qualite:N", title="Qualité"),
                alt.Tooltip("nombre:Q", title="Publications"),
            ],
        )
        .properties(height=320)
    )
    st.altair_chart(graphique_volume, use_container_width=True)
    st.caption(
        "Hauteur totale : nombre de publications enregistrées lors de ce passage. "
        "En bleu, celles qui disposent d'une image utilisable par le détecteur. "
        "Une barre qui s'effondre signale un flux appauvri ou une source en panne."
    )

st.divider()

# --- Durée par etape ----------------------------------------------------
st.subheader("Où passe le temps")

exclure_echecs = st.checkbox(
    "Écarter les étapes en échec du calcul des durées",
    value=True,
    help="Une étape qui s'arrête en erreur s'interrompt en cours de route : sa durée "
    "ne représente pas le temps de travail réel.",
)
vue_durees = durees[~durees["state"].isin(ETATS_ECHEC)] if exclure_echecs else durees

if vue_durees.empty:
    st.warning("Aucune durée mesurée pour ce filtre.")
else:
    moyennes = (
        vue_durees.groupby("etape")["duration"]
        .agg(moyenne="mean", maximum="max", executions="count")
        .reset_index()
    )

    barres = (
        alt.Chart(moyennes)
        .mark_bar(color="#4c78a8")
        .encode(
            x=alt.X("moyenne:Q", title="Durée moyenne, en secondes"),
            y=alt.Y("etape:N", title="Étape du pipeline", sort="-x"),
            tooltip=[
                alt.Tooltip("etape:N", title="Étape"),
                alt.Tooltip("moyenne:Q", title="Durée moyenne, s", format=".1f"),
                alt.Tooltip("maximum:Q", title="Durée maximale, s", format=".1f"),
                alt.Tooltip("executions:Q", title="Exécutions mesurées"),
            ],
        )
    )
    pointes = (
        alt.Chart(moyennes)
        .mark_tick(color="#f58518", thickness=2.5, size=26)
        .encode(x="maximum:Q", y=alt.Y("etape:N", sort="-x"))
    )
    st.altair_chart((barres + pointes).properties(height=240), use_container_width=True)
    st.caption(
        "Barre bleue : durée moyenne de l'étape. Trait orange : durée la plus longue "
        "observée. Un écart important entre les deux indique une étape irrégulière, "
        "généralement dépendante du réseau."
    )

st.divider()

# --- Detail ------------------------------------------------------------
with st.expander("Données détaillées"):
    st.markdown("**Volumes par fichier**")
    colonne_brut, colonne_retenu = st.columns(2)
    colonne_brut.caption("Reçu du flux RSS, data/raw")
    colonne_brut.dataframe(brut, use_container_width=True, hide_index=True)
    colonne_retenu.caption("Retenu après transformation, data/processed")
    colonne_retenu.dataframe(retenu, use_container_width=True, hide_index=True)

    st.markdown("**Exécutions des tâches Airflow**")
    st.dataframe(
        taches[["run_id", "etape", "state", "start_date", "end_date", "duration"]],
        use_container_width=True,
        hide_index=True,
    )

    st.markdown("**Publications par exécution**")
    st.dataframe(publications, use_container_width=True, hide_index=True)

    st.caption(
        f"Source : base {CONNEXION['dbname']} sur {CONNEXION['host']}:{CONNEXION['port']}, "
        f"schéma {SCHEMA}. Lecture seule, actualisation toutes les 60 secondes."
    )