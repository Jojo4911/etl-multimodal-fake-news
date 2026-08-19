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