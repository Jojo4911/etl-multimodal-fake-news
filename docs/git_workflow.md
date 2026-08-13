# Flux git du projet P12

> Document de travail, pas un livrable OpenClassrooms. Aucune case de la fiche d'auto-évaluation ne l'évalue.
> Objectif unique : produire un historique lisible sur GitHub, exploitable en candidature.
> À placer dans `docs/git_workflow.md`.

## Principe

Modèle le plus simple qui fasse le travail : `main` plus une branche courte par étape du planning.
Pas de `develop`, pas de `release`, pas de `hotfix`. Ces branches servent à coordonner plusieurs personnes, il n'y en a qu'une ici.

- `main` est toujours dans un état démontrable. C'est la branche que le mentor verra au bilan.
- Une branche par étape, durée de vie de 1 à 2 jours maximum.
- Une PR par branche, fusionnée avec un **merge commit** (pas de squash) pour conserver les commits par tâche, qui montrent la granularité du travail.
- Un commit par tâche du planning, message en anglais à l'impératif.

## Table des branches

| Branche | Tâches | Jours | Contenu | PR fusionnée le |
|---|---|---|---|---|
| `setup/airflow-stack` | T01, T02, T03 | J1 | Docker Compose Airflow 3.3.1, arborescence, README, DAG hello world | soir du J1, 13/08 |
| `docs/rapport-exploration` | T04, T05 | J2 | Qualification des 3 sources, rapport d'exploration | soir du J2, 14/08 |
| `feat/extraction-rss` | T06, T07 | J3, J4 | Module d'extraction, robustesse, images, logs | soir du J4, 18/08 |
| `feat/pipeline-transformation` | T08 | J5 | Pipeline nettoyage, normalisation, export | soir du J5, 19/08 |
| `docs/schema-donnees` | T09 | J6 | Schéma conceptuel Mermaid | soir du J6, 20/08 |
| `feat/dag-etl` | T10, T11 | J6, J7 | DAG à 3 tâches, chargement Postgres, preuves d'exécution | soir du J7, 21/08 |
| `feat/dashboard-kpi` | T12 | J8 | Tableau de bord Streamlit | soir du J8, 24/08 |
| `docs/plan-monitoring` | T13 | J9 | Plan de monitoring | soir du J9, 25/08 |
| `chore/livrables` | T15 | J10 | Renommage des 7 livrables, packaging | soir du J10, 26/08 |

Les branches `A1` à `A7` du bloc d'apprentissage Airflow **n'existent pas**. Ce bloc ne produit aucun code livré. S'il produit des notes, elles vont dans `docs/notes_apprentissage_airflow.md`, commitées sur la branche de l'étape en cours.

## Cas particulier du J6

Deux branches vivent en parallèle le 20/08 : `docs/schema-donnees` (T09) et `feat/dag-etl` (T10).
Elles ne touchent pas les mêmes fichiers, aucun conflit possible. Fusionne `docs/schema-donnees` en fin de J6, garde `feat/dag-etl` ouverte jusqu'au J7 puisque T11 la complète.

## Cycle de travail, à répéter à chaque étape

Ouverture de branche, au début de l'étape :

```bash
git checkout main
git pull
git checkout -b feat/extraction-rss
```

Commit, à la fin de chaque tâche :

```bash
git add .
git commit -m "Add RSS extraction module with feedparser"
```

Publication et PR, à la fin de l'étape :

```bash
git push -u origin feat/extraction-rss
```

Puis sur GitHub : "Compare & pull request", titre au format `Étape 2 : scripts d'extraction`, description en trois lignes maximum listant les cases de la fiche couvertes. Fusion avec "Create a merge commit". Suppression de la branche distante proposée par GitHub : accepte.

Retour sur main :

```bash
git checkout main
git pull
git branch -d feat/extraction-rss
```

## Ce que la description de PR doit contenir

Trois lignes, pas plus. Exemple pour `feat/extraction-rss` :

```
Extraction automatisée depuis un flux RSS, texte plus image.
Cases couvertes : C1.5, C1.6, C1.7, C1.8.
Testé : exécution complète sans intervention manuelle.
```

Cette discipline a un effet secondaire utile : au J10, la liste des PR est déjà le brouillon de ta fiche d'auto-évaluation et de ta démonstration.

## Tag final

Après la fusion de `chore/livrables`, le 26/08 :

```bash
git tag -a v1.0-livrables -m "Deliverables submitted to OpenClassrooms"
git push origin v1.0-livrables
```

## Ce qu'on ne fait pas

Décisions actées, à ne pas rouvrir en cours de projet :

- Pas de CI GitHub Actions. Coût réel, zéro case, et rien à tester automatiquement ici.
- Pas de `git rebase -i` ni de réécriture d'historique. Un historique honnête vaut mieux qu'un historique cosmétique, et le risque de perte de travail est réel.
- Pas de conventional commits avec scopes et types. Impératif présent en anglais, une ligne, suffit.
- Pas de branche par tâche. Une par étape.
- Pas de fichiers de données ni de logs versionnés. `data/`, `logs/`, `config/` et `.env` sont dans le `.gitignore`, ils y restent.
