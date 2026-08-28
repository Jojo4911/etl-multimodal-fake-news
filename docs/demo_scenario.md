# Scénario de démonstration live : pipeline ETL multimodal CheckIt.AI

> Projet 12, session de bilan mentor. Durée cible : 10 minutes.
> Terminal : PowerShell 5.1. Répertoire de travail : `C:\dev\p12-etl-multimodal`.
> Objectif de rubrique : rendre visibles les cases C3.1 (le DAG s'exécute sans
> erreur) et C3.2 (les tâches sont bien séparées), plus C4.1 et C4.3 sur le
> tableau de bord.

---

## Préparation, à faire 15 minutes avant la session

Ces étapes ne se font PAS devant le mentor. Elles évitent les temps morts.

### P1. Relance à froid de la stack

```powershell
cd C:\dev\p12-etl-multimodal
docker compose down
docker compose up -d
```

**Ne jamais utiliser `docker compose down -v`.** Le volume `postgres-db-volume` porte les données et les preuves d'exécution du T11. Le supprimer détruit la démonstration d'idempotence.

### P2. Attendre que les services soient sains

```powershell
docker compose ps
```

Attendre que les services affichent `healthy` ou `running`. Compter 1 à 2 minutes après un `up -d`. Le service `airflow-dag-processor` est celui qui parse les DAGs : s'il n'est pas parti, le DAG n'apparaîtra pas dans l'UI.

### P3. Vérifier l'accès à l'UI

Ouvrir `http://localhost:8080`, se connecter avec `airflow` / `airflow`.
Laisser l'onglet ouvert sur la vue du DAG `etl_fake_news`.

### P4. Relever le compteur de départ

```powershell
docker compose exec postgres psql -U airflow -d airflow -c "SELECT COUNT(*) FROM checkit.publications;"
```

**Noter le nombre ici avant la session :** 56............

Ce chiffre est le point de comparaison de la séquence 3. Il change à chaque run, il faut donc le relever le jour même et ne jamais réutiliser une valeur écrite ici. Dernier relevé : **56 le 28/08 à 16h52**, après la répétition.

Le compteur seul ne prouve rien. Ce qui prouve l'idempotence, ce sont les métriques retournées par la tâche `load` : lignes ajoutées contre lignes mises à jour. Le compteur sert uniquement à vérifier que le total est cohérent avec ces métriques, en direct et de tête.

### P5. Lancer le tableau de bord dans une SECONDE fenêtre PowerShell

```powershell
cd C:\dev\p12-etl-multimodal
uv run streamlit run src/dashboard/app.py
```

Laisser tourner. Ouvrir l'onglet navigateur, le laisser en arrière-plan.
Le lancer devant le mentor coûte 20 secondes de silence pour rien.

### P6. Vérifier l'état de pause du DAG

Dans l'UI, regarder si le DAG `etl_fake_news` est en pause ou actif. S'il est actif avec `schedule="@daily"` et `catchup=False`, un run planifié peut se déclencher tout seul au redémarrage de la stack. Ce n'est pas un problème, mais il faut le savoir pour ne pas confondre un run automatique avec le run déclenché en direct pendant la séquence 3.

---

## Séquence 0 : ouverture, 0:00 à 1:30

Écrans : le dépôt ouvert dans l'éditeur, `dags/etl_fake_news_dag.py` affiché.

Points d'appui du discours :

- Le besoin CheckIt.AI : alimenter en continu un détecteur de fake news avec des publications d'actualité qui portent à la fois du texte et une image.
- La source industrialisée est le flux RSS de RFI. Une seule source industrialisée, trois sources documentées dans le rapport d'exploration. Justification : pas de clé d'API, pas de quota, images exposées en `media:content`, francophone, volume de texte le plus élevé des flux testés.
- La chaîne de formats : JSON Lines en brut, CSV après transformation, images en fichiers sur disque référencées par chemin, PostgreSQL en stockage final.
- Le pipeline est orchestré par Airflow 3.3.1 en Docker Compose.

Ne pas dérouler ici le détail du rapport d'exploration. Il est déposé, le mentor l'a lu. Cette séquence pose le contexte, elle ne le rejoue pas.

---

## Séquence 1 : le DAG dans l'UI, 1:30 à 3:30

Écran : UI Airflow, vue **Graph** du DAG `etl_fake_news`.

À montrer, dans cet ordre :

1. **Les trois nœuds du graphe** : `extract`, `transform`, `load`, reliés en chaîne linéaire. C'est la case C3.2, les tâches sont bien séparées.
2. **Ce que fait chaque tâche**, une phrase chacune :
   - `extract` interroge le flux RFI, écrit un fichier JSON Lines dans `data/raw/` et télécharge les images dans `data/images/`.
   - `transform` lit ce fichier, nettoie le texte, valide les images, écarte les entrées non conformes et écrit un CSV dans `data/processed/`.
   - `load` lit le CSV et fait un upsert dans `checkit.publications`.
3. **Le passage d'information entre tâches** : basculer sur le code, montrer que `tache_transform` fait un `ti.xcom_pull(task_ids="extract")` et reçoit une **chaîne de caractères, un chemin de fichier**. Jamais la charge utile. Phrase à tenir : faire transiter les images ou le JSON complet par XCom ferait échouer le DAG de façon illisible, XCom stocke ses valeurs dans la base de métadonnées d'Airflow.
4. **La configuration de planification** : `schedule="@daily"`, `catchup=False`, `start_date=datetime(2026, 8, 20)`. Une phrase : le
   `catchup=False` évite de rejouer tous les intervalles écoulés depuis la date de départ au premier démarrage.
5. **Montrer l'historique des runs**, argument fort à ne pas oublier : le DAG n'a pas seulement été déclenché à la main pour la démonstration, il tourne sur son déclenchement planifié quotidien, sans intervention. Ouvrir la liste des runs et montrer un run automatique passé au vert.

---

## Séquence 2 : déclenchement live et logs, 3:30 à 6:00

Écran : UI Airflow.

1. **Déclencher le DAG** avec le bouton Trigger. Rester sur la vue Grid pour voir les trois cases passer au vert. Durée observée : environ 5 secondes par run complet.
2. **Ouvrir les logs de la tâche `transform`** dans l'UI. Montrer les lignes de journalisation qui donnent le volume avant et après filtrage.
3. **Ouvrir les logs de la tâche `load`**. Montrer les métriques retournées : lignes lues, lignes en base avant, lignes en base après, lignes ajoutées, lignes mises à jour.

**Secours si les logs de l'UI n'affichent que l'en-tête structuré** (incident déjà rencontré, ne pas improviser) :

```powershell
docker compose exec airflow-worker bash -lc 'find /opt/airflow/logs -name "*.log" -newermt "-30 minutes" -path "*load*"'
```

puis, avec le chemin retourné :

```powershell
docker compose exec airflow-worker bash -lc "cat '<chemin_retourne>'"
```

---

## Séquence 3 : la base remplie et l'idempotence, 6:00 à 8:00

Écran : PowerShell, première fenêtre.

1. **Le compteur après le run**

```powershell
docker compose exec postgres psql -U airflow -d airflow -c "SELECT COUNT(*) FROM checkit.publications;"
```

Comparer au chiffre relevé en P4. C'est ici que se démontre l'idempotence.

2. **Le contenu réel de la table**

```powershell
docker compose exec postgres psql -U airflow -d airflow -c "SELECT id, LEFT(title, 50) AS titre, published_at, image_valid FROM checkit.publications ORDER BY fetched_at DESC LIMIT 5;"
```

Montrer que chaque ligne porte un titre propre, une date et une image validée.
Affichage vérifié en répétition : le tableau tient dans la largeur d'une
fenêtre PowerShell standard, et les accents s'affichent correctement.

Dire en passant que la colonne `image_valid` affiche `t` pour `true`, c'est
l'abréviation booléenne de PostgreSQL. Sans cette phrase, la colonne ressemble
à un code obscur.

3. **Expliquer le résultat**, c'est le point de la séquence :
   - La clé primaire `id` est un `sha1(article_url)` tronqué à 16 caractères.
     Elle est **déterministe** : la même URL d'article produit toujours le même
     identifiant, quel que soit le moment du run.
   - Le chargement fait un `INSERT ... ON CONFLICT (id) DO UPDATE`. Un article
     déjà présent est mis à jour, pas dupliqué.
   - **Preuve principale, mesurée le 28/08** : un seul run a lu 23 entrées,
     inséré 13 articles neufs et mis à jour 10 articles déjà connus, faisant
     passer la base de 43 à 56. Les deux comportements dans une seule
     exécution, et l'arithmétique se vérifie de tête devant le mentor :
     13 plus 10 égale 23 lues, 43 plus 13 égale 56. Aucun doublon.
   - Preuve complémentaire, mesurée au T11 sur deux runs consécutifs : le
     premier a inséré 22 lignes dans une base vide, le second a lu 22 lignes
     et n'en a ajouté qu'une seule, avec 21 mises à jour.
   - Les preuves écrites sont dans `docs/preuves/` : `run1_load.log`,
     `run2_load.log`, `postgres_apres_run2.txt` et les captures de l'UI.

---

## Séquence 4 : le tableau de bord KPI, 8:00 à 10:00

Écran : onglet Streamlit déjà ouvert, lancé en P5.

Rappeler en une phrase que le tableau de bord lit la base en lecture seule et
tourne hors Docker, connecté au Postgres publié sur `127.0.0.1:5433`. Il
n'écrit rien, il n'interfère pas avec le pipeline.

Les cinq indicateurs, à commenter en langage métier, pas en langage technique.
C'est la case C4.3, compréhensible par un non technicien :

| Indicateur | Ce qu'il surveille | Ordre de grandeur observé |
|---|---|---|
| Pourcentage d'articles retenus | La qualité de la source en entrée | 95 à 100 % |
| Pourcentage d'articles illustrés | La couverture multimodale | 100 % par construction |
| Publications en base | Le volume accumulé | 56 au 28/08, croissant |
| Pourcentage d'étapes en échec | La santé du pipeline | 0 % |
| Durée moyenne d'exécution | Le coût en temps d'un run | environ 5 s |

Ne pas annoncer de valeur chiffrée au mentor sans avoir regardé l'écran : ces
indicateurs bougent à chaque run. Les ordres de grandeur ci-dessus servent à
repérer une anomalie, pas à être récités.

**Point d'honnêteté à porter soi-même, avant que le mentor ne le trouve** :
l'indicateur « articles illustrés » est tautologique. Il ne peut afficher que
100 %, puisque `transform.py` écarte en amont toute entrée sans image valide.
Il a été requalifié en contrôle d'invariant dans le plan de monitoring, section
3.4 : s'il descend sous 100 %, c'est que le filtre amont a cessé de fonctionner.
Le signaler soi-même est un point fort, pas un aveu.

---

## L'avertissement FERNET_KEY, à traiter une fois

Chaque commande `docker compose` affiche neuf lignes d'avertissement sur la
variable `FERNET_KEY` non renseignée. Le mentor les verra.

**Ne pas les masquer** avec une redirection d'erreur : ça masquerait aussi une
vraie erreur en pleine démonstration. Les traiter par une phrase, dite une
seule fois à la première commande : la clé Fernet sert à chiffrer les
Connections stockées par Airflow, je n'en utilise aucune, les identifiants de
base passent par le fichier `.env` injecté par Compose et lus depuis
l'environnement. C'est documenté dans mes notes d'apprentissage A5.

Signaler soi-même un avertissement qu'on sait expliquer est un point fort.

---

## Chiffres à connaître par coeur

- Le flux RFI sert **23 entrées par appel**, de façon stable.
- **Le taux de rétention n'est pas une constante**, il dépend du contenu du
  flux au moment de l'appel : 23 sur 23 le 19/08, 22 sur 23 le 21/08, 23 sur
  23 le 28/08. Ne pas annoncer un pourcentage figé, dire que le filtre écarte
  les entrées sans texte suffisant ou sans image valide, et lire l'écran.
- Colonnes de la table, dans l'ordre : `id`, `source`, `article_url`, `title`,
  `text_clean`, `published_at`, `image_path`, `image_valid`, `fetched_at`.
- Run du 28/08 : 23 lues, base 43 vers 56, 13 ajoutées, 10 mises à jour.
- Runs du T11 : 22 lues, base 0 vers 22, puis 22 lues, base 22 vers 23.

---

## Incidents anticipés et réaction

| Incident | Réaction |
|---|---|
| Le DAG n'apparaît pas dans l'UI | Interroger le service qui parse les DAGs : `docker compose logs airflow-dag-processor --tail 50` |
| Les logs de l'UI sont tronqués | Passer par le fichier, commande de la séquence 2 |
| Le flux RFI ne répond pas | La tâche `extract` lève une erreur explicite. Montrer les preuves de `docs/preuves/` à la place, le run du T11 est daté et journalisé |
| Streamlit ne se connecte pas | Vérifier que le service `postgres` tourne, le port publié est `127.0.0.1:5433` |
| Le run est déjà passé au vert tout seul | C'est le run planifié `@daily`. Le dire, et déclencher quand même un run manuel |

---

## Après la démonstration : les 4 autres points du bilan

Voir les notes de bilan mentor, produites séparément en fin de T14.