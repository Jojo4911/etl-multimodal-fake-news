# Preuves d'execution du flux ETL

DAG `etl_fake_news`, Airflow 3.3.1, 27/08/2026.

## Double execution consecutive

| Run           | Lignes lues | Base avant | Base apres | Ajoutees | Mises à jour |
|---------------|-------------|------------|------------|----------|--------------|
| 1 (15:24 UTC) | 22          | 0          | 22         | 22       | 0            |
| 2 (15:31 UTC) | 22          | 22         | 23         | 1        | 21           |

Le second run n'a produit ni erreur ni doublon : 21 des 22 entrées étaient déjà présentes et ont été mises à jour par `ON CONFLICT (id) DO UPDATE`. La ligne supplementaire est un article publie par RFI entre les deux executions. Le flux RSS étant une fenêtre glissante, la base accumule alors que le flux reste a volume constant.

Le flux brut contient 23 entrées à chaque appel. Le pipeline de transformation en retient 22, soit un taux d'entrées valides de 95,7 % sur cette serie, contre 100 % le 19/08.

L'unicité est garantie structurellement par la PRIMARY KEY sur `id`, dérive de `sha1(article_url)[:16]`.

## Fichiers

- `run1_load.log`, `run2_load.log` : logs bruts de la tache `load`.
- `postgres_apres_run2.txt` : comptage des lignes et des id distincts.
- `dag_graph_trois_taches.png` : séparation des tâches (C3.2).
- `dag_deux_runs_verts.png` : deux executions en succés (C3.1).