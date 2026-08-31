# Pipeline ETL multimodal pour détection de fake news

Pipeline d'acquisition automatisée de publications d'actualité **texte et image**, orchestré par Apache Airflow. Il ingère un flux RSS, nettoie et valide les publications, puis les charge dans PostgreSQL sous une forme directement exploitable par un modèle de classification.

Le cas d'usage retenu est l'alimentation continue d'un détecteur de fausses informations, qui a besoin d'un corpus à jour, structuré et associant chaque texte à son image.

---

## Ce que fait le pipeline

Trois étapes, une tâche Airflow par étape.

| Étape | Module | Entrée | Sortie |
|---|---|---|---|
| `extract` | `src/extract_rss.py` | Flux RSS de RFI | JSON Lines dans `data/raw/`, images dans `data/images/` |
| `transform` | `src/transform.py` | Le fichier JSON Lines | CSV validé dans `data/processed/` |
| `load` | `src/load.py` | Le CSV | Table `checkit.publications` |

Les tâches se transmettent des **chemins de fichiers** via XCom, jamais les données elles-mêmes. Faire transiter une image ou un JSON complet par XCom saturerait la base de métadonnées d'Airflow.

### Transformation

L'étape de transformation nettoie le texte, normalise les dates, déduplique et **écarte toute entrée sans texte suffisant ou sans image valide**. Une publication qui entre en base porte donc toujours ses deux modalités.

### Idempotence

La clé primaire est un hachage déterministe de l'URL de l'article, tronqué à 16 caractères. Le chargement fait un `INSERT ... ON CONFLICT (id) DO UPDATE`.

Conséquence : rejouer le pipeline ne crée aucun doublon, un article déjà connu est mis à jour. La garantie est portée par le moteur de base de données, pas par la discipline du code appelant.

---

## Modèle de données

Table `checkit.publications` :

| Colonne | Rôle |
|---|---|
| `id` | Clé primaire, hachage de l'URL de l'article |
| `source` | Identifiant de la source d'origine |
| `article_url` | URL canonique de l'article, clé naturelle |
| `title` | Titre de la publication |
| `text_clean` | Corps de texte nettoyé, entrée du modèle |
| `published_at` | Date de publication déclarée par la source |
| `image_path` | Chemin de l'image associée sur le disque |
| `image_valid` | Résultat du contrôle de validité de l'image |
| `fetched_at` | Horodatage de l'ingestion |

Le modèle conceptuel, indépendant de toute technologie de stockage, est documenté dans `docs/`.

---

## Arborescence

```
dags/          DAG Airflow
src/           modules d'extraction, de transformation et de chargement
src/dashboard/ tableau de bord Streamlit
docs/          documentation et preuves d'exécution
data/          données produites, non versionné
```

---

## Environnement

- **Apache Airflow 3.3.1** en Docker Compose, à partir du quickstart officiel.
- **PostgreSQL 16**, schéma dédié `checkit`.
- **Python 3.13** en local, géré par uv. Airflow n'est pas installé en local, il tourne uniquement dans les conteneurs.
- `feedparser` est ajouté aux conteneurs via `_PIP_ADDITIONAL_REQUIREMENTS`.
- `PYTHONPATH` pointe sur `/opt/airflow/src` pour que le DAG importe les modules par leur nom.

Airflow 3 a déplacé `PythonOperator` vers `airflow.providers.standard.operators.python`. Les tutoriels écrits pour Airflow 2 échouent à l'import.

---

## Démarrage

1. Créer un fichier `.env` à la racine, non versionné (voir Configuration).
2. `docker compose up airflow-init`
3. `docker compose up -d`
4. Interface Airflow : http://localhost:8080

Pour repartir d'un état propre sans perdre les données :

```
docker compose down
docker compose up -d
```

`docker compose down -v` supprime le volume PostgreSQL et donc les données ingérées.

---

## Configuration

Aucun identifiant ne figure dans le code. Les modules lisent leur configuration depuis l'environnement, ce qui permet au même code de tourner dans n'importe quel environnement sans modification.

La base est atteinte depuis deux endroits qui ne la voient pas à la même adresse : les conteneurs Airflow l'appellent par son nom de service sur le port interne, le tableau de bord Streamlit tourne sur la machine hôte et passe par le port publié. Les identifiants, eux, sont communs.

### Depuis les conteneurs

`docker-compose.yaml` construit six variables `CHECKIT_PG_*` et les injecte dans l'environnement des services Airflow. Toutes ont une valeur par défaut, et l'utilisateur, le mot de passe et la base sont dérivés des variables `POSTGRES_*` standard :

```
CHECKIT_PG_HOST      défaut postgres
CHECKIT_PG_PORT      défaut 5432
CHECKIT_PG_USER      reprend POSTGRES_USER
CHECKIT_PG_PASSWORD  reprend POSTGRES_PASSWORD
CHECKIT_PG_DB        reprend POSTGRES_DB
CHECKIT_PG_SCHEMA    défaut checkit
```

Le fichier `.env` n'a donc besoin de porter que ce qui diffère de ces valeurs par défaut, plus `AIRFLOW_UID` et les identifiants `POSTGRES_*`.

### Depuis la machine hôte

Le tableau de bord lit le fichier `.env` puis l'environnement du processus, ce dernier l'emportant en cas de conflit. Il utilise **deux variables dédiées** pour l'adresse, afin de ne pas entrer en collision avec celle des conteneurs :

```
CHECKIT_DASHBOARD_PG_HOST  défaut 127.0.0.1
CHECKIT_DASHBOARD_PG_PORT  défaut 5433
```

Le port `5433` est celui que le service PostgreSQL publie sur la boucle locale.
Le port interne `5432` n'est pas exposé.

---

## Exécution

Le DAG `etl_fake_news` est planifié en `@daily`, avec `catchup=False` : au démarrage, il ne rejoue pas les intervalles écoulés depuis sa date de départ. Il peut aussi être déclenché manuellement depuis l'interface.

### Tableau de bord

Le tableau de bord Streamlit tourne hors Docker et lit la base en lecture seule. Depuis la racine du dépôt :

```
uv run streamlit run src/dashboard/app.py
```

Il expose le volume ingéré, le taux de publications retenues, la couverture en images, le taux d'échec des étapes et la durée moyenne d'exécution.

### Requêtes directes

Le port PostgreSQL interne n'est pas exposé. Les requêtes en ligne de commande passent par le conteneur :

```
docker compose exec postgres psql -U airflow -d airflow -c "SELECT COUNT(*) FROM checkit.publications;"
```

---

## Documentation

Le dossier `docs/` contient le rapport d'exploration des sources, le schéma de données, le plan de monitoring, des notes d'apprentissage sur Airflow, ainsi qu'un dossier `preuves/` regroupant journaux et captures d'exécutions successives du pipeline.