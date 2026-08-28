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

## A5 : Connections, Variables, secrets

**Principe** : séparer la configuration du code. Le code décrit comment joindre une base, la configuration décrit laquelle.

**Trois problèmes d'une chaine de connexion en dur dans un DAG :**
1. Le secret part sur GitHub, et reste dans l'historique meme après suppression.
2. Un DAG par environnement, donc deux fichiers à maintenir.
3. Le plus grave : la rotation devient impossible en pratique. Changer le mot de passe demande une PR et un redeploiement, donc personne ne le change.

**Configuration contre secret.** `CHECKIT_PG_HOST` vaut `postgres` : sa fuite n'a aucune consequence, c'est de la configuration. Le mot de passe donne l'accès : c'est un secret. Cycles de vie differents. C'est la raison d'être de deux mecanismes distincts dans Airflow.

**Les quatre emplacements :**

| Mecanisme                          | Usage                                | Limite                                          |
|------------------------------------|--------------------------------------|-------------------------------------------------|
| Variables d'environnement          | Config simple, portable hors Airflow | Visible dans le conteneur, exige un `down`/`up` |
| Airflow Variables                  | Config qui bouge, editable en UI     | Pas structuree pour un accès externe            |
| Airflow Connections                | Acces externes, mot de passe chiffré | Depend de la `FERNET_KEY`                       |
| Backend de secrets (Vault, AWS SM) | Production                           | Infrastructure dedièe                           |

**Ce que le chiffrement d'une Connection protege reellement.** La base de metadonnées d'Airflow est ici le même Postgres que la table `publications`. La `FERNET_KEY` vit dans l'environnement des conteneurs Airflow, a côté. Le chiffrement protège donc contre un accès au fichier de base seul : sauvegarde, dump, disque recuperé. Il ne protège pas contre une compromission de la machine. Protection AU REPOS, pas contre un attaquant déjà dans l'infrastructure.

**FERNET_KEY vide sur cette stack.** Comportement par defaut du Docker Compose, pas un défaut du projet. Consequence concrète : Airflow ne peut pas chiffrer les mots de passe de Connections, donc le mecanisme de Connections est inutilisable en l'état.

**Choix retenu et sa defense.** Identifiants dans le `.env`, injectés par Compose, lus par `os.environ`. Justifié par le perimètre (zero case), par l'infrastructure (pas de `FERNET_KEY`), et par la portabilité (`load.py` testable hors Airflow). Limite assumée : pas de rotation. En production, Connection Airflow, puis backend de secrets a l'échelle.

**Perimètre** : aucune Connection créée, `load.py` inchangé, `FERNET_KEY` non modifiée. Cette note alimente la section 6 du rapport
et le bilan mentor.

## A6 : TaskFlow API contre opérateurs classiques

### Ce que TaskFlow est réellement

Ce n'est pas un autre moteur d'exécution. Le décorateur `@task` construit un opérateur Python à partir d'une fonction ordinaire. À l'exécution, on obtient exactement les mêmes opérateurs et les mêmes XCom qu'avec des `PythonOperator` écrits à la main. C'est une façon d'écrire, pas une façon d'exécuter.

En Airflow 3, les décorateurs vivent au même endroit que le `DAG` : `from airflow.sdk import dag, task`.

### Le mécanisme : le XComArg

C'est le point central du bloc. Quand Airflow parse un fichier de DAG, aucune tâche ne s'exécute : il construit seulement le graphe. Le corps des fonctions décorées n'est donc pas exécuté à ce moment-là.

Dans `chemin = extract()`, la variable `chemin` ne contient ni le résultat de la fonction, ni rien de vide. Elle contient un **XComArg** : un objet qui signifie « la future valeur de retour de la tâche `extract` ». Une référence, pas une donnée.

Quand ce XComArg est passé à `transform(chemin)`, Airflow le reconnaît, en déduit que `transform` dépend de `extract`, et pose l'arête du graphe. La dépendance est donc établie **en avant**, à la construction du graphe, et non résolue à rebours. Ce n'est qu'à l'exécution qu'Airflow résout la référence, en effectuant le `xcom_pull` qu'on écrivait manuellement. 

Formule à retenir : **TaskFlow ne fait pas transiter des données entre les tâches, il fait transiter des promesses de données, et les résout au moment de l'exécution.**

### Ce que ça change, et ce que ça ne change pas

Ce que ça change :

- Les dépendances se déduisent du passage d'arguments. La ligne `extract >> transform >> load` devient inutile pour une chaîne linéaire de données.
- XCom devient invisible. Plus de `ti.xcom_pull(task_ids="tache_transform")`, donc plus de clé textuelle à taper juste. La signature de la fonction remplace la chaîne de caractères.

Ce que ça ne change pas :

- Aucun gain ni aucune perte de performance. Ce sont les mêmes opérateurs et les mêmes XCom. Invoquer la performance dans un sens ou dans l'autre est une erreur d'analyse.
- La gestion des valeurs absentes. Si `extract_rss.run()` retourne `None`, ce `None` traverse XCom et arrive comme argument de `transform`, qui doit toujours poser sa garde explicite. Point culturel appris ici : l'idiome Airflow pour ce cas n'est pas de retourner `None` mais de lever une `AirflowSkipException` dans la tâche amont, ce qui fait passer les tâches avales à l'état `skipped` au lieu de leur transmettre une valeur vide.

Enfin, tout n'est pas décorable : les opérateurs des providers, les sensors et les opérateurs SQL n'ont pas d'équivalent TaskFlow. Un DAG de production mélange les deux styles, et la chaîne `>>` reste nécessaire pour exprimer une dépendance de contrôle, quand une tâche doit s'exécuter après une autre sans consommer sa sortie.

### L'argument contre TaskFlow qui vaut au delà de ce projet

Masquer une contrainte rend la contrainte plus facile à violer. `donnees = extract()` a exactement l'allure d'une affectation Python ordinaire alors que ce n'en est pas une. TaskFlow rend donc l'anti-pattern du gros payload en XCom, vu en A1, plus facile à commettre qu'avec un `xcom_push` explicite. C'est l'argument qu'un ingénieur avance contre TaskFlow pour une équipe junior.

Corollaire sur la lisibilité, souvent mal formulée : TaskFlow est plus lisible pour qui connaît Python, et moins lisible pour qui cherche à comprendre ce qu'Airflow fait, puisque le mécanisme est justement caché.

### Décision prise sur ce projet : pas de réécriture

J'ai comparé les deux styles et gardé les `PythonOperator` classiques.

TaskFlow produit les mêmes opérateurs et les mêmes XCom, il n'apporte aucun gain d'exécution : ce qu'il apporte, c'est de la concision, en déduisant les dépendances du passage d'arguments. Sur un DAG à trois tâches linéaires, ce gain est marginal, et la réécriture d'un code déjà validé sur deux exécutions consécutives introduit un risque de régression sans contrepartie.

L'argument décisif est celui de la rubrique : les deux seules cases évaluées sont « mon DAG s'exécute sans erreur dans Airflow » et « mes tâches sont bien séparées ». Ni l'une ni l'autre ne dépend du style d'écriture. Une réécriture consommerait du budget en échange de zéro case gagnée.

Si le DAG devait grossir à une dizaine de tâches avec du branchement, je basculerais : la chaîne de dépendances explicite devient alors la principale source d'erreurs, et c'est précisément ce que TaskFlow supprime.

### À retester à froid en T14

La notion de XComArg m'a été donnée, pas construite. Question de contrôle : « dans un DAG TaskFlow, que contient la variable `chemin` au moment où Airflow parse le fichier, et comment l'arête du graphe est-elle posée ? »