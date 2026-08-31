# Notes de bilan mentor : projet 12

> Support de préparation, pas un livrable. Couvre les temps 2 à 5 de la session
> de bilan. Le temps 1, la démonstration, est traité dans `demo_scenario.md`.

---

## Temps 2 : la fiche d'auto-évaluation et la colonne Notes

Préparée en T15. Trois points de discussion à y porter, rappelés ici pour
mémoire :

1. Le KPI « articles illustrés » est tautologique, requalifié en contrôle
   d'invariant dans le plan de monitoring, section 3.4.
2. Sept heures d'apprentissage Airflow hors rubrique, blocs A1 à A7.
3. Les seuils du plan de monitoring sont calés sur deux points de mesure
   seulement, d'où la revue mensuelle de recalibrage prévue.

---

## Temps 3 : les difficultés rencontrées

### La difficulté principale : Airflow dans le détail

Airflow était le seul outil réellement nouveau du projet. Faire tourner un DAG
à trois tâches n'a pas été difficile. Ce qui a demandé du travail, c'est le
détail de son fonctionnement, et notamment trois points :

- **La sémantique de planification.** Comprendre que l'étiquette d'un run
  désigne une fenêtre de temps et pas un instant d'exécution, et que le run
  d'une période se déclenche à la fermeture de cette période. C'est le sujet
  qui a demandé le plus de reprises.
- **XCom.** Comprendre que les valeurs transitent par la base de métadonnées,
  ce qui rend l'anti-pattern du gros payload structurel et non stylistique.
- **La distinction TaskFlow et opérateurs classiques.** Comprendre à quel
  moment le graphe est construit et ce que contient réellement une variable
  au moment du parsing.

Ces trois points ont été travaillés en blocs d'ancrage dédiés, avec un premier
jet à froid avant toute lecture de documentation. Ils sont les seuls du projet
à avoir consommé leur estimation complète.

### Le point de dépassement horaire unique : le chargement Postgres

Une seule tâche du projet a dépassé son estimation de façon marquée : le
chargement vers PostgreSQL, 2,87 h réalisées contre 1,5 h estimées. Le reste
du projet a sous-consommé.

### Les pièges d'installation, purgés en premier

Le principal risque calendaire identifié avant le démarrage était le setup
d'Airflow. Il a été traité au premier jour, avant tout autre travail. Deux
pièges concrets :

- Airflow 3 a déplacé `PythonOperator` vers
  `airflow.providers.standard.operators.python`. La quasi-totalité des
  tutoriels en ligne utilisent l'ancien chemin et échouent à l'import. La
  version installée a été relevée avant d'ouvrir le moindre tutoriel.
- Airflow 3 a un service séparé qui parse les DAGs. C'est lui qu'il faut
  interroger en cas d'erreur d'import, pas le scheduler.

### Les frictions d'environnement, non techniques mais coûteuses

Windows et PowerShell 5.1 ont produit du bruit récurrent : BOM ajouté en
écriture de fichier UTF-8, mojibake en lecture sans option d'encodage,
conversion de chemins sous Git Bash. Aucun de ces points n'est un défaut de
code, mais ils ont consommé du temps d'attention.

### La difficulté qui n'est pas technique : le calendrier

Le projet est très en dessous de son budget en heures et en retard sur sa
fenêtre calendaire. Voir le temps 5.

---

## Temps 4 : les points forts

### La discipline de périmètre

La rubrique demande trois sources pour le rapport d'exploration et **une seule**
pour le script automatisé. Trois sources ont été documentées et comparées, une
seule a été industrialisée. Automatiser une deuxième source aurait été du
travail non évalué.

Même logique sur la sécurité de la base : la mission la cite en point de
vigilance, la rubrique ne lui accorde aucune case. Le sujet est traité par un
paragraphe argumenté dans le rapport, pas par une couche de code.

Même logique sur TaskFlow : le comparatif a été fait et argumenté, le DAG livré
n'a pas été réécrit. Les deux cases concernées étaient déjà acquises.

### Le choix de la source, mesuré et non subi

RFI n'a pas été retenu par habitude. Plusieurs flux francophones ont été
échantillonnés et comparés sur le volume de texte disponible par entrée. Le
flux retenu est celui qui offrait le meilleur volume, avec des images exposées
en `media:content`.

### L'idempotence, garantie par la base et non par le code

La clé primaire est un hachage déterministe de l'URL de l'article. Le
chargement fait un `INSERT ... ON CONFLICT (id) DO UPDATE`. Conséquence : la
non-duplication n'est pas une propriété de la discipline applicative, elle est
imposée par le moteur. Mesuré le 28/08 sur un run unique : 23 entrées lues,
13 insertions, 10 mises à jour, base de 43 à 56, zéro doublon.

### Un défaut d'indicateur détecté et déclaré spontanément

Le KPI « pourcentage d'articles illustrés » ne peut afficher que 100 %, puisque
la transformation écarte en amont toute entrée sans image valide. Le défaut a
été identifié en cours de projet, sans y être invité. Il n'a pas été masqué ni
reconstruit : il a été requalifié en contrôle d'invariant dans le plan de
monitoring.

### Sept heures d'apprentissage assumées hors rubrique

Airflow pèse deux cases sur vingt-trois, soit 9 % de la validation. Un budget
d'apprentissage dédié de sept heures a été investi au-delà de ce que la
rubrique exige, en décision explicite et bornée : ce bloc n'a produit aucune
ligne de code livré, uniquement de la compréhension. Sortie écrite dans
`docs/airflow_learning_notes.md`.

---

## Temps 5 : les actions à mener ensuite

### Consolider ce qui a été compris mais pas encore reconstruit

Quatre notions ont été retestées à froid avant ce bilan. Trois sont solides.
Une reste fragile : **la distinction entre configuration par l'environnement
et codage en dur**, dans le contexte des Connections Airflow et de la gestion
des secrets. Le réflexe qui revient sous pression est de justifier par le
`.gitignore`, ce qui est une conséquence et non l'argument.

### Ce que le pipeline ne fait pas, et qui est identifié

- **Le fenêtrage temporel n'est pas exploité.** L'extraction interroge le flux
  au moment où elle s'exécute, elle ne filtre pas sur l'intervalle du run. Le
  DAG est planifié, pas fenêtré. C'est cohérent avec une source qui n'expose
  que ses dernières entrées, mais c'est une limite à connaître.
- **Aucun retry, timeout ni callback dans le DAG livré.** Le plan de monitoring
  décrit la cible de production, il ne l'implémente pas. C'est un choix de
  périmètre, pas un oubli.
- **Les seuils de monitoring reposent sur deux points de mesure.** D'où la
  revue mensuelle de recalibrage inscrite au plan.

### Le point calendaire, à assumer tel quel

Le projet est **très en dessous de son budget en heures et en retard sur sa
fenêtre calendaire**. C'est un motif déjà observé sur les projets précédents :
sous budget en heures, en retard en jours. Il ne se corrige pas en comprimant
la préparation du bilan, il se remonte au pilotage du parcours.

---

## Risque à préparer : l'usage de l'IA dans la production du code

La mission demande explicitement d'être capable d'expliquer son cheminement et
ses décisions techniques quand l'IA a été utilisée. Le mentor peut ouvrir un
module et demander de le commenter.

**État réel, à ne pas maquiller :** le projet a distingué trois catégories de
code. Les blocs d'ancrage, les sept blocs d'apprentissage Airflow et le DAG,
ont été écrits par jets successifs, avec un premier jet à froid systématique.
Les modules utilitaires, `extract_rss.py`, `transform.py`, `load.py` et le
tableau de bord, ont été produits directement, sans jet préalable.

**Conséquence à anticiper :** la connaissance de ces trois modules est moins
fraîche que celle du DAG. Avant le bilan, prévoir une relecture ciblée de 20
minutes, sur trois questions et pas davantage :

1. Dans `transform.py`, quels sont les critères exacts de rejet d'une entrée,
   et quelle est la valeur du seuil de longueur de texte ?
2. Dans `load.py`, où se construit la requête d'upsert et sur quelle colonne
   porte le `ON CONFLICT` ?
3. Dans `extract_rss.py`, comment l'URL de l'image est-elle récupérée dans
   l'entrée RSS, et que se passe-t-il si elle est absente ?

Savoir répondre à ces trois questions couvre l'essentiel de ce qu'un mentor
demandera. Ne pas relire les trois modules ligne à ligne, ce serait du travail
non évalué.
