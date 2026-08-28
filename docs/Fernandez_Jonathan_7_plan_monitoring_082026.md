# Plan de monitoring du pipeline d'ingestion multimodale

**Projet** : CheckIt.AI, pipeline d'acquisition de données multimodales pour la détection de fake news
**Pipeline concerné** : DAG `etl_fake_news_dag`, Apache Airflow 3.3.1
**Source** : flux RSS de RFI, `https://www.rfi.fr/fr/rss`
**Stockage** : PostgreSQL, schéma `checkit`, table `publications`
**Auteur** : Jonathan Fernandez
**Date** : août 2026

---

## 1. Objet, périmètre et posture

### 1.1 Le pipeline surveillé

Le DAG `etl_fake_news_dag` est planifié en `@daily` et enchaîne trois tâches séparées :

| Tâche | Rôle | Sortie |
|---|---|---|
| `extract` | Lecture du flux RSS RFI, téléchargement des images | `data/raw/rfi_<horodatage>.jsonl` et `data/images/<id>.jpg` |
| `transform` | Nettoyage du texte, validation des images, normalisation des dates, déduplication | `data/processed/<fichier>.csv` |
| `load` | Upsert dans `checkit.publications` | dictionnaire de métriques du run |

### 1.2 Le risque métier réel

Le corpus produit par ce pipeline alimente l'entraînement du détecteur de fake news de CheckIt.AI. La panne franche n'est pas le danger principal : elle se voit, le DAG passe au rouge, quelqu'un s'en aperçoit.

Le danger est la **dégradation silencieuse**. Un flux qui se vide, des images qui cessent d'être téléchargeables, une base qui n'est plus alimentée alors que toutes les tâches restent vertes. Dans ce cas, le modèle continue de s'entraîner sur un corpus rétréci, appauvri ou périmé, et personne n'est alerté puisque rien n'a échoué.

Ce plan est donc construit autour de la détection du silence autant que de la détection de l'échec. C'est ce qui distingue un plan de monitoring d'une simple surveillance de statut de tâche.

### 1.3 Périmètre exclu

Ce plan ne couvre pas :

- la qualité de prédiction du modèle aval, qui relève d'un plan de monitoring de modèle distinct ;
- la santé de l'infrastructure hôte, Docker et système de fichiers, hors indicateur d'espace disque ;
- la supervision d'Airflow lui-même, scheduler et dag-processor, au-delà de son effet observable sur la fraîcheur de la donnée produite.

### 1.4 Statut d'implémentation, à lire avant la suite

**Le DAG livré n'implémente aucun retry, aucun timeout, aucun callback d'alerte.** C'est un choix assumé et non un oubli.

Le livrable évalué porte sur la séparation des tâches et sur l'exécution sans erreur. Instrumenter une chaîne d'alerte réelle sur un pipeline de démonstration exécuté en local produirait du code sans contrepartie opérationnelle : aucune astreinte pour recevoir l'alerte, aucun canal de production pour l'acheminer.

Ce document décrit donc **ce qui serait paramétré en production, avec quelles valeurs et sur quelle justification**. Chaque section distingue explicitement ce qui est observable aujourd'hui de ce qui relève de la cible.

---

## 2. Ce qui est observable aujourd'hui

Cinq sources d'observation existent déjà, sans outillage supplémentaire.

| # | Source | Contenu | Rythme |
|---|---|---|---|
| S1 | Logs de tâche Airflow | Volume traité, entrées écartées, erreurs réseau, motif de rejet | Par run, par tâche |
| S2 | Métriques retournées par `tache_load` | `lignes_lues`, `lignes_avant`, `lignes_apres`, `lignes_ajoutees`, `lignes_mises_a_jour`, poussées en XCom sous `return_value` | Par run |
| S3 | Fichiers de `data/raw` et `data/processed` | Comparaison des volumes avant et après transformation | Par run |
| S4 | Base PostgreSQL | Table `checkit.publications` et table `task_instance` de la base de métadonnées Airflow | Continu |
| S5 | Tableau de bord Streamlit, `src/dashboard/app.py` | Agrégation de S3 et S4 en cinq cartes lisibles par un non technicien | À la demande |

Aucun outil externe n'est requis. Le plan ne prévoit ni Prometheus, ni Grafana, ni collecteur de métriques dédié : à l'échelle d'un pipeline quotidien à trois tâches et une source, ces outils coûteraient plus en exploitation qu'ils ne rapporteraient en visibilité.

---

## 3. Indicateurs suivis et seuils d'alerte

### 3.1 Note de calibrage, à lire avant le tableau

Les seuils ci-dessous sont calés sur les mesures réelles des 19/08/2026 et 28/08/2026. **L'historique est court** : deux points de mesure sur le taux de rétention, un seul régime stable observé sur le volume du flux, et aucune mesure espacée de 24 heures sur le renouvellement.

Chaque seuil porte donc sa justification chiffrée et une échéance de recalibrage. Poser des seuils ronds sans observation aurait été plus lisible et beaucoup moins utile : un seuil arbitraire produit soit du bruit, soit du silence, et dans les deux cas on cesse de le regarder.

### 3.2 Tableau des indicateurs

| Code | Indicateur | Source | Valeur observée | Avertissement | Critique |
|---|---|---|---|---|---|
| I1 | Entrées lues dans le flux, par run | S1, S3 | 23, stable | < 20 | < 10 ou 0 |
| I2 | Taux d'entrées retenues après transformation | S2, S3 | 95,7 % le 28/08, 100 % le 19/08 | < 90 % | < 75 % |
| I3 | Publications ajoutées en base, par run quotidien | S2 | non mesuré sur 24 h | 0 sur un run | 0 sur deux runs consécutifs |
| I4 | Fraîcheur : âge du `fetched_at` le plus récent | S4 | quelques minutes après un run | > 26 h | > 50 h |
| I5 | Durée d'un passage complet du DAG | S4 | 5,1 s en moyenne | > 30 s | > 120 s |
| I6 | Taux d'étapes en échec sur un run | S4 | 0 % | > 0 % | échec sur deux jours consécutifs |
| I7 | Espace disque libre sur le volume portant `data/` | hôte | non instrumenté | < 20 % libre | < 10 % libre |

### 3.3 Justification de chaque seuil

**I1, entrées lues.** Le flux RFI sert 23 entrées par appel de façon stable sur toutes les mesures effectuées. Un flux RSS est une fenêtre glissante de taille fixe : une baisse du nombre d'entrées ne traduit pas une actualité moins fournie, elle traduit une réponse tronquée, un flux partiellement cassé ou un changement côté éditeur. Le seuil d'avertissement à 20 tolère une variation de l'ordre de 13 % autour de la seule valeur stable connue. Le seuil critique à 10 ou 0 correspond à une réponse manifestement anormale ou vide.

**I2, taux d'entrées retenues.** C'est l'indicateur central de qualité de la donnée. Sur 23 entrées, 22 retenues donnent 95,7 %, 21 donnent 91,3 %, 20 donnent 87,0 %. Le seuil d'avertissement à 90 % tolère donc deux rejets et se déclenche au troisième. Le seuil critique à 75 % correspond à six rejets ou plus, ce qui ne s'explique plus par des cas isolés mais par un changement structurel : format du flux modifié, images devenues inaccessibles, encodage du texte altéré. L'intérêt de cet indicateur est démontré par les mesures elles-mêmes : la valeur est passée de 100 % à 95,7 % entre deux dates, il mesure donc bien quelque chose de réel et non un invariant de construction.

**I3, publications ajoutées.** Sur deux exécutions rapprochées de quelques minutes, le second run n'a ajouté qu'une publication et en a mis à jour 21. C'est le comportement attendu d'un upsert sur un flux qui n'a pas eu le temps de se renouveler. Sur un run quotidien en revanche, RFI publie de nouveaux articles en 24 heures : un run qui n'ajoute **aucune** publication signale soit un flux figé, soit un DAG qui rejoue une donnée périmée. Le seuil ne peut pas encore être chiffré au-delà de zéro, faute de mesure espacée de 24 heures. Il sera précisé au premier recalibrage mensuel, avec pour cible le premier décile du nombre d'ajouts quotidiens observé sur 30 jours.

**I4, fraîcheur.** C'est l'indicateur anti-silence, celui qui détecte la panne qui ne lève aucune erreur. Avec un `schedule="@daily"`, la donnée la plus récente en base ne devrait jamais avoir plus de 24 heures, plus une marge d'exécution. Le seuil d'avertissement à 26 h correspond à un run manqué ou fortement retardé. Le seuil critique à 50 h correspond à deux runs manqués, soit un pipeline effectivement arrêté. Sans cet indicateur, un scheduler arrêté ne produit aucune tâche en échec et donc aucune alerte : le tableau de bord afficherait des chiffres parfaitement verts sur une base qui ne bouge plus.

**I5, durée d'exécution.** La durée moyenne mesurée d'un passage complet est de 5,1 secondes. Elle est dominée par le réseau : une requête RSS et le téléchargement des images. Le seuil d'avertissement à 30 secondes, environ six fois le nominal, signale une dégradation réseau ou un ralentissement volontaire côté RFI. Le seuil critique à 120 secondes est la valeur qui serait retenue comme `execution_timeout` en production, avec une marge très large sur le nominal pour éviter de couper un run simplement lent.

**I6, taux d'étapes en échec.** Aucun échec de tâche n'a été observé. Avec trois tâches par run, une seule défaillance représente 33 % du run : il n'existe aucune bande de tolérance possible à ce volume. Tout échec est donc un avertissement immédiat. Deux jours consécutifs en échec constituent un incident critique, car le corpus cesse alors d'être alimenté.

**I7, espace disque.** Les images sont conservées sur disque et référencées par chemin, elles s'accumulent à raison d'une vingtaine par jour sans purge. Le poids moyen d'une image n'a pas été mesuré et ne sera pas estimé au jugé ici. Le seuil porte donc sur le pourcentage d'espace libre du volume, mesurable immédiatement et sans calibrage préalable. La première vérification hebdomadaire instrumente la mesure du volume de `data/images/` et permettra de projeter une date de saturation.

### 3.4 Cas particulier : le taux d'articles illustrés

Le tableau de bord affiche une carte « articles illustrés » à 100 %. **Cette valeur est structurellement incapable de descendre en dessous de 100 %**, puisque `transform.py` écarte en amont toute entrée dont l'image n'est pas valide. L'indicateur est tautologique.

Il est conservé, mais requalifié : ce n'est pas un indicateur de qualité, c'est un **contrôle d'invariant**. Toute valeur différente de 100 % signalerait une régression dans la logique de filtrage de `transform.py`, c'est-à-dire un défaut de code et non un défaut de donnée. À ce titre, le seuil d'alerte est l'égalité stricte : tout écart à 100 % est un avertissement.

L'information de qualité recherchée, la proportion de publications utilisables, est déjà portée par I2. Le texte d'aide de la carte du tableau de bord énonce explicitement cette limite.

---

## 4. Gestion des erreurs et politique de retry

### 4.1 Préalable non négociable : l'idempotence

**Un retry n'est pas une option de configuration, c'est une conséquence de l'idempotence.** Relancer automatiquement une tâche qui n'est pas rejouable transforme une panne passagère en corruption de données.

Le pipeline satisfait cette condition :

- la clé primaire `id` est déterministe, `sha1(article_url)[:16]`, elle ne dépend ni de l'horodatage ni de l'ordre de traitement ;
- le chargement utilise `INSERT ... ON CONFLICT (id) DO UPDATE`, et non `DO NOTHING`, parce que RFI met à jour ses articles après publication ;
- les écritures sur disque sont déterministes, une image réécrite écrase la précédente à l'identique.

La preuve est mesurée : deux exécutions consécutives, la première a inséré 22 lignes dans une base vide, la seconde a ajouté 1 ligne et mis à jour 21, sans erreur ni doublon. La base est passée de 22 à 23 lignes, pas de 22 à 44.

**Conséquence pour ce plan** : toute politique de retry décrite ci-dessous n'est valide que tant que cette propriété est maintenue. Une modification de la clé ou du mode d'écriture invalide la politique de retry et doit déclencher sa révision.

### 4.2 Trois classes de panne, trois instruments distincts

Les confondre est l'erreur classique. Chaque classe appelle un instrument différent, et un instrument mal choisi ne corrige rien.

| Classe de panne | Symptôme | Instrument | Pourquoi les autres ne marchent pas |
|---|---|---|---|
| Panne passagère | La tâche échoue, une nouvelle tentative réussit : coupure réseau, HTTP 503 de RFI, base momentanément indisponible | `retries` et `retry_delay` | Un timeout ne répare rien, il coupe plus tôt |
| Tâche bloquée | La tâche ne se termine jamais : connexion réseau suspendue sans réponse | `execution_timeout` | Un retry sur une tâche bloquée empile des exécutions suspendues |
| Tâche lente mais verte | La tâche réussit, en dix fois le temps habituel | Alerte sur la durée, Deadline Alerts | Ni retry ni timeout ne se déclenchent, la tâche réussit |

Point de version à connaître : le mécanisme de SLA d'Airflow 2 a été retiré dans Airflow 3. Le troisième cas se traite désormais avec les **Deadline Alerts**, ou à défaut par une vérification externe de la durée sur la table `task_instance`.

### 4.3 Paramétrage cible, par tâche

À mettre en place en production. Non implémenté dans le DAG livré, voir 1.4.

| Tâche | `retries` | `retry_delay` | `execution_timeout` | Justification |
|---|---|---|---|---|
| `extract` | 2 | 5 min, avec backoff exponentiel | 5 min | Seule tâche réellement exposée au réseau : appel RSS et téléchargement des images. Les pannes y sont majoritairement passagères. Le timeout est plus large que sur les autres car le volume d'images domine la durée. |
| `transform` | 0 | sans objet | 2 min | Traitement local et déterministe. Un échec est un défaut de code ou un changement de format d'entrée : le rejeu reproduit l'échec à l'identique et ne fait que retarder le diagnostic. |
| `load` | 2 | 2 min | 2 min | PostgreSQL peut être momentanément indisponible, redémarrage ou saturation de connexions. Le rejeu est sûr grâce à l'upsert idempotent. |

Le choix de `retries=0` sur `transform` est délibéré et se défend en entretien : **retenter sans raison de croire au succès est un anti-pattern**, il masque le défaut et allonge le délai de détection.

### 4.4 Erreurs de donnée contre erreurs de pipeline

Distinction structurante, déjà implémentée dans le code livré.

- **Erreur de donnée** : une entrée du flux est inexploitable, image en 404, texte trop court, date illisible. L'entrée est écartée, le motif est journalisé, **le run continue**. Une image manquante sur 23 ne doit pas faire échouer une ingestion quotidienne.
- **Erreur de pipeline** : le flux est injoignable, le CSV n'est pas produit, la base refuse la connexion. La tâche échoue et le run s'arrête.

Le lien entre les deux est I2 : les erreurs de donnée n'échouent pas, elles se **comptent**. C'est leur accumulation qui déclenche l'alerte, via le taux de rétention, et non leur occurrence unitaire.

### 4.5 Limite connue de la gestion d'erreur actuelle

Les entrées rejetées sont comptées mais **non conservées**. Si I2 se dégrade, il faut relancer une extraction pour comprendre pourquoi. En production, la première amélioration à porter serait la journalisation du motif de rejet par entrée, dans un fichier de rebut, afin que le diagnostic ne dépende pas d'un rejeu.

---

## 5. Rythmes de vérification

| Rythme | Responsable | Durée | Contenu | Déclencheur d'action |
|---|---|---|---|---|
| **Par run**, automatique | Aucun, machine | 0 | Statut des trois tâches, métriques retournées par `load`, journalisation des rejets | Échec de tâche, franchissement d'un seuil critique |
| **Quotidien** | Ingénieur data d'astreinte | 2 min | Ouverture du tableau de bord : articles retenus, publications en base, étapes en échec, durée moyenne, fraîcheur | Tout seuil d'avertissement franchi |
| **Hebdomadaire** | Ingénieur data d'astreinte | 15 min | Tendance de I2 sur 7 jours, volume de `data/images/` et espace libre, relecture des journaux d'`extract` à la recherche d'avertissements récurrents non bloquants, vérification que la structure du flux RFI n'a pas changé | Tendance dégradée même sans franchissement de seuil |
| **Mensuel** | Ingénieur data et lead technique | 30 min | **Recalibrage des seuils** sur l'historique réel de 30 jours, en particulier I1, I2 et I3. Revue des incidents du mois. Décision sur la politique de rétention des images | Seuils obsolètes, incident récurrent |

La revue mensuelle n'est pas un supplément de confort : les seuils de la section 3 sont posés sur deux points de mesure, ils sont provisoires par construction. Un plan de monitoring qui ne prévoit pas son propre recalibrage vieillit mal et finit ignoré.

---

## 6. Responsabilités et canal d'alerte

### 6.1 Rôles

CheckIt.AI est une structure réduite, l'organisation d'astreinte doit rester légère.

| Rôle | Périmètre | Engagement |
|---|---|---|
| Ingénieur data d'astreinte, rotation hebdomadaire | Vérification quotidienne, traitement des avertissements, premier diagnostic des incidents critiques | Prise en compte d'un avertissement sous 1 jour ouvré |
| Lead technique | Escalade des incidents critiques, arbitrage en cas de changement de structure du flux source, validation du recalibrage mensuel | Prise en compte d'un incident critique sous 2 h ouvrées |

### 6.2 Niveaux de sévérité et acheminement

| Niveau | Déclencheur | Canal | Délai attendu |
|---|---|---|---|
| **Information** | Run nominal terminé | Aucun. Consultable au tableau de bord | Sans objet |
| **Avertissement** | Franchissement d'un seuil d'avertissement de la section 3 | Message automatique dans le canal Slack `#checkit-data-alertes` | Traitement sous 1 jour ouvré |
| **Critique** | Franchissement d'un seuil critique, ou échec de tâche | Slack avec mention de l'astreinte, et courriel à l'alias `data-alertes@checkit.ai` | Prise en compte sous 2 h ouvrées |

En production, l'acheminement s'appuierait sur les callbacks Airflow, `on_failure_callback` au niveau du DAG pour les échecs, et une tâche de contrôle finale évaluant les seuils de la section 3 pour les avertissements. Les identifiants du webhook Slack suivraient la règle déjà appliquée sur ce projet : variables d'environnement injectées par le conteneur, jamais de valeur en dur dans un fichier de DAG versionné.

### 6.3 Conduite à tenir sur incident critique

1. Constater : ouvrir le tableau de bord et identifier quel indicateur a franchi son seuil.
2. Qualifier : erreur de donnée en accumulation, indicateur I2, ou erreur de pipeline, indicateur I6.
3. Relancer une fois si l'échec paraît passager. La relance est sûre, le pipeline est idempotent.
4. Si le second essai échoue, ne pas relancer une troisième fois : diagnostiquer par les journaux de la tâche en échec.
5. Escalader au lead technique si la cause est un changement côté source, format du flux ou politique d'accès aux images.
6. Consigner l'incident pour la revue mensuelle.

---

## 7. Limites connues et évolutions

Ces limites sont assumées et documentées, elles ne sont pas des oublis.

| Limite | Effet | Évolution proposée |
|---|---|---|
| Seuils calés sur deux points de mesure | Risque de faux positifs les premières semaines | Recalibrage mensuel, déjà inscrit au rythme de vérification |
| Source unique, RFI | Une indisponibilité de RFI est une indisponibilité du pipeline, sans redondance | Arbitrage assumé pour ce périmètre. L'ajout d'une seconde source rendrait I1 et I2 mesurables par source et permettrait un basculement |
| Indicateur d'articles illustrés tautologique | Ne détecte pas la dégradation, seulement la régression de code | Requalifié en contrôle d'invariant, section 3.4. Ne pas le reconstruire, l'information utile est portée par I2 |
| Alerting non implémenté | Les seuils sont vérifiés à l'œil au rythme quotidien | Callbacks Airflow et tâche de contrôle finale, décrits en 6.2 |
| Entrées rejetées non conservées | Diagnostic de I2 impossible sans rejeu | Fichier de rebut avec motif de rejet, décrit en 4.5 |
| Pas de politique de rétention des images | Croissance monotone du disque | Décision à prendre à la revue mensuelle, une fois le volume réel mesuré |

---

## 8. Synthèse en une page

- Le pipeline est quotidien, à trois tâches, sur une source unique, et alimente un corpus d'entraînement.
- Le risque dominant n'est pas la panne franche mais la **dégradation silencieuse**, d'où le poids donné à l'indicateur de fraîcheur I4.
- **Sept indicateurs**, chacun avec un seuil d'avertissement et un seuil critique chiffrés à partir des mesures réelles des 19/08 et 28/08, chacun avec sa justification et son échéance de recalibrage.
- **Trois classes de panne, trois instruments** : retries pour le passager, `execution_timeout` pour le bloqué, alerte sur durée pour le lent mais vert. Le retry n'est légitime que parce que le pipeline est idempotent, et cette propriété est mesurée.
- **Quatre rythmes** : automatique par run, 2 minutes par jour, 15 minutes par semaine, 30 minutes par mois dont le recalibrage des seuils.
- **Deux rôles, trois niveaux de sévérité**, avec des délais de prise en compte explicites.
- Le plan **décrit la cible de production**. Le DAG livré n'implémente ni retry, ni timeout, ni callback : choix assumé, justifié en section 1.4.
