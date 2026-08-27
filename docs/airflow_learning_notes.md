# Notes d'apprentissage Airflow, projet P12

## A1 : XCom, usage légitime et anti-pattern du gros payload

### Mécanisme
Nous allons utiliser **XCom** et la sérialisation des données pour échanger des informations entre les différentes tâches d’un DAG.
L'idée est d'utiliser la base de données de métadonnées d'Airflow, dans notre cas PostgreSQL pour cette communication.

### Pourquoi XCom existe
Chaque tâche s'exécute dans un processus distinct, potentiellement sur une machine distincte. Deux tâches ne partagent aucune mémoire, une variable Python de la première n'existe pas dans la seconde. La base de métadonnées est le seul canal commun, XCom est donc un mécanisme de communication inter-processus.

### L'anti-pattern du gros payload
Vouloir insérer une grosse image encodée en base 64 au travers des XCOM va casser à trois endroits :
1. À l'**écriture**, deux cas. Bytes bruts : TypeError, l'objet n'est pas sérialisable en JSON. Encodés en base64 : ça passe, mais avec 33 % de volume en plus, et chaque run laisse ces dizaines de mégaoctets dans la table xcom, qu'aucune purge automatique ne nettoie par défaut. La base de métadonnées gonfle jusqu'à saturer le volume Docker.
2. À la **lecture** le worker va devoir désérialiser toute l'image intégralement dans la mémoire et va être tué par l’OOM killer.
3. Dans l’**UI** La page du Xcom va tenter d'afficher la valeur, et va se figer.

### Règle appliquée dans ce projet
Donc, la règle qui sera appliquée dans ce projet sera de transférer les chemins des images au lieu de l'image complète.
La tâche extract écrit le .jsonl et les images sur le volume monté, puis pousse en XCom la seule chaîne data/raw/rfi_20260813T185118Z.jsonl. La tâche transform reçoit ce chemin et ouvre le fichier elle-même.

### Ordre de grandeur
XCom est dimensionné pour des scalaires et de petits dicts, de l'ordre du kilo-octet. Au-delà, on passe une référence.

## A2 : idempotence et rejouabilité

### Définition
Une tâche est idempotente quand la relancer laisse le système dans le même état final utile, quel que soit le nombre d'exécutions. La notion porte sur l'état du système, pas sur la valeur retournée : un `INSERT` qui crée un doublon retourne pourtant « succès » les deux fois.

Nuance importante, vue sur mon propre pipeline : l'identité bit à bit des artefacts intermédiaires n'est pas requise. Deux runs produisent deux fichiers `.jsonl` différents, avec un horodatage différent dans le nom, et c'est acceptable. Ce qui doit rester stable, c'est l'état observable qui compte : 23 images sur disque, 23 lignes en base, un article présent une seule fois.

### Les points de casse de mon pipeline au second run
- **Images** : aucun problème. Le nom de fichier dérive de l'identifiant de publication, et `download_image()` sort immédiatement si le fichier existe déjà. Ni doublon, ni re-téléchargement.
- **data/raw/** : accumulation. Chaque run écrit un nouveau `.jsonl` horodaté contenant largement les mêmes articles. Ce n'est pas une erreur en soi, mais cela pose la question de savoir quel fichier la tâche `transform` doit lire.
- **Base Postgres** : le vrai point de casse. Un `INSERT` simple sur deux runs consécutifs donne 46 lignes pour 23 articles. C'est le scénario qui échoue en démonstration live devant le mentor, puisque la case C3.1 sera vérifiée en relançant le DAG.

### Clé naturelle
`id = sha1(article_url)[:16]` est déterministe et dérivé de la donnée elle-même : le même article donne toujours le même identifiant, quel que soit le moment du run ou la machine. C'est ce qui en fait une clé naturelle, par opposition à un `SERIAL` auto-incrémenté, qui attribuerait un nouvel identifiant à chaque insertion du même article et rendrait la détection de doublon impossible.

### Solution appliquée
Contrainte `PRIMARY KEY` sur `id`, sans laquelle Postgres ne sait pas détecter le conflit et lève une erreur au lieu de faire un upsert. Puis :

    INSERT INTO publications (...) VALUES (...)
    ON CONFLICT (id) DO UPDATE SET
        title      = EXCLUDED.title,
        text       = EXCLUDED.text,
        image_path = EXCLUDED.image_path,
        fetched_at = EXCLUDED.fetched_at;

`DO UPDATE` et non `DO NOTHING` : RFI met ses articles à jour en direct, un article « EN DIRECT » voit son titre et son texte évoluer alors que son URL, donc son identifiant, ne change pas. `DO NOTHING` figerait la première version captée, `DO UPDATE` conserve la plus fraîche.

### Lien avec A1
Les deux notions se rejoignent sur un même point : la tâche `transform` lit le chemin retourné par `extract` et transmis par XCom, pas le contenu du dossier `data/raw/`. Cela règle d'un coup l'anti-pattern du gros payload et l'ambiguïté du fichier à traiter au second run.

## A3 : sémantique de planification

**Date** : 19/08/2026. Bloc d'apprentissage Airflow, 3/7. Aucun livrable.

### Le principe fondateur

Airflow ne planifie pas des instants, il planifie des **tranches de temps**.
Un `@daily` découpe le calendrier en intervalles fermés de 24 h : `[1er janvier 00:00, 2 janvier 00:00[`, puis `[2 janvier, 3 janvier[`, etc.
Un run correspond à une tranche, pas à un moment de lancement.

### Trois dates à ne pas confondre

| Notion | Ce que c'est |
|---|---|
| `data_interval_start`, alias `logical_date` | début de la tranche couverte, c'est l'étiquette du run |
| `data_interval_end` | fin de la tranche |
| heure réelle d'exécution | quand le code tourne effectivement |

### Règle de déclenchement

Un run se déclenche **quand sa tranche est terminée**, jamais pendant.
Le run étiqueté du 3 mars tourne le **4 mars à 00:00**, à sa `data_interval_end`. On ne peut pas traiter les données du 3 mars avant que le 3 mars soit fini.

Conséquence pratique : un DAG `@daily` avec `start_date` au 19 août, activé le 19 août, ne produit **aucun run avant le 20 août à 00:00**. En démo, le DAG apparaît actif et vide. Le déclenchement manuel via `Trigger DAG` contourne ce décalage, c'est le mode utilisé pour la démonstration live.

### Pourquoi cette conception

L'étiquette rend le run **reproductible** : elle indique au code quelles données traiter. Rejoué six mois plus tard, le run du 3 mars doit produire le même résultat. Un code qui s'appuierait sur `datetime.now()` produirait un résultat différent à chaque rejeu, donc non reproductible.
Lien direct avec A2 : l'idempotence porte sur l'état final du système, et l'étiquette temporelle est ce qui rend cet état final déterministe.

### `catchup=True` contre `catchup=False`

- `catchup=True` : à l'activation, Airflow crée un run pour **chaque intervalle terminé** depuis `start_date`. Du 1er janvier au 19 août, cela fait 230 runs créés d'un coup. Ils ne partent pas tous en parallèle, ils sont mis en file et limités par `max_active_runs`, valeur 16 par défaut.
- `catchup=False` : Airflow ne planifie que le **dernier intervalle terminé**, puis attend la fin de l'intervalle courant pour le suivant. Ce n'est pas « un run pour aujourd'hui » : le run se produit à la prochaine `data_interval_end`, pas à l'activation.

### Backfill

Rejeu délibéré d'une plage d'intervalles passés, déclenché à la main. Il ne se justifie que si la source expose des données historiques adressables par date. Ce n'est pas le cas ici.

### Décision actée pour T10

```python
schedule="@daily",
catchup=False,
start_date=datetime(2026, 8, 20),
```

**Justification** : le flux RSS de RFI n'expose que son état courant, aucune donnée historique n'est adressable par date. Rejouer un intervalle passé produirait les articles d'aujourd'hui sous une étiquette d'hier, ce qui fausserait la donnée sans rien rattraper. Le rattrapage n'a donc pas de sens sur cette source et `catchup=False` s'impose.

### Point de discussion pour le bilan mentor

La confusion entre `logical_date` et heure d'exécution est l'erreur de lecture classique sur Airflow. Savoir qu'un run `@daily` du 3 mars tourne le 4 mars, et pouvoir dire pourquoi, est le marqueur de compréhension attendu sur ce sujet.

## A4 : retries, timeouts, alerting, SLA

Trois pannes distinctes, trois instruments distincts.

| Situation                                  | Instrument               | Effet sur la tâche         |
|--------------------------------------------|--------------------------|----------------------------|
| Echec passager (réseau, base indisponible) | `retries`, `retry_delay` | Retente, finit verte       |
| Ne se termine jamais                       | `execution_timeout`      | Tuée, passe en echec       |
| Se termine mais trop tard                  | Alerte sur duree         | Reste verte, alerte a côté |

**Le retry est une consequence de l'idempotence, pas une option de configuration.** Activer `retries` sur une tache non idempotente automatise la corruption. Avant l'`ON CONFLICT (id) DO UPDATE` du T11, un retry sur `load` aurait fait echouer la tache sur violation de la PRIMARY KEY, puis echouer chaque nouvelle tentative. Sans cle primaire, il aurait dupliqué.

**Application au pipeline `etl_fake_news` :**

- `extract` : depend de RFI, service externe. Echec passager probable. Candidat naturel au retry. Timeout applicatif déjà posé dans `requests`, un `execution_timeout` Airflow serait un second filet à un autre étage.
- `transform` : lecture locale, règles déterministes. Echec definitif si échec. Le retry n'apporte rien.
- `load` : mixte. Erreur de schema ou de type = definitif. Echec de connexion Postgres = passager, et c'est un des echecs transitoires les plus frequents en production. Retry pertinent, rendu sur par l'écriture idempotente.

**Alerting** : sans `on_failure_callback`, un echec nocturne n'existe que dans l'UI. Le callback est le crochet ou brancher mail, Slack ou PagerDuty.

**Point de version** : le mécanisme SLA d'Airflow 2 (`sla` sur l'operateur, `sla_miss_callback` sur le DAG) a ete retiré dans
Airflow 3, remplacé par les Deadline Alerts. Les exemples en ligne visent majoritairement Airflow 2.

**Périmetre** : aucun de ces parametres n'est implementé dans le DAG livré. Les 2 cases C3.1 et C3.2 sont couvertes sans eux. Cette note alimente le plan de monitoring du J9.

