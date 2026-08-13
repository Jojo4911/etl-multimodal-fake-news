# Rapport d'exploration de sources

**Projet** : pipeline d'acquisition de données multimodales pour un détecteur de fake news
**Commanditaire** : CheckIt.AI
**Auteur** : Jonathan Fernandez, ingénieur data junior
**Date** : 14 août 2026
**Livrable** : 1 sur 7

---

## 1. Contexte et objectif

CheckIt.AI développe un moteur de détection automatique de désinformation. L'enrichissement de ce moteur suppose un flux régulier de publications comportant **à la fois du texte et une image associée**, condition nécessaire à l'entraînement et à l'évaluation d'un modèle multimodal.

### 1.1 Pourquoi la multimodalité est un besoin, pas un confort

Une part importante de la désinformation contemporaine ne repose pas sur un texte faux isolé, mais sur la **relation entre un texte et une image**. Trois familles de cas dominent :

- **image authentique sortie de son contexte** : une photographie réelle, correctement datée et localisée à l'origine, republiée comme illustration d'un événement sans rapport. C'est le cas le plus fréquent et le plus difficile à détecter par le texte seul ;
- **image altérée ou générée** : montage, retouche, ou synthèse par un modèle génératif, associée à un récit plausible ;
- **texte trompeur illustré par une image authentique** : l'image, vraie, sert de caution de véracité au texte, faux.

Dans les trois cas, l'analyse du texte seul ou de l'image seule est insuffisante. Le signal exploitable est la **cohérence, ou l'incohérence, entre les deux modalités**. Une source qui fournit du texte et des images sans garantir leur appariement au sein d'une même publication est donc inutilisable pour ce cas d'usage.

### 1.2 Distinction préalable : opinion controversée et désinformation

Une **opinion controversée** est un jugement subjectif qui divise, y compris lorsqu'il heurte les normes établies, et relève de la liberté d'expression. La **désinformation** désigne une information objectivement fausse, diffusée dans l'intention de tromper. Seule la seconde entre dans le périmètre du détecteur. Cette distinction a une conséquence directe sur la qualification des sources : une source dont les labels confondent les deux catégories introduirait un biais de censure dans le modèle, et a été traitée comme un défaut de qualité de labels.

### 1.3 Objectif de ce rapport

Identifier et qualifier au moins trois sources de données multimodales pertinentes, puis désigner celle qui sera industrialisée dans le pipeline automatisé.

---

## 2. Méthode de qualification

Neuf critères ont été appliqués à chaque source :

| Critère | Question posée |
|---|---|
| Rôle | Quelle fonction la source remplit-elle dans le dispositif ? |
| Modalités | Texte et image sont-ils présents et appariés dans la même entrée ? |
| Format brut | Sous quelle forme la donnée est-elle servie ? |
| Langue | Quelle langue, et compatible avec le marché visé ? |
| Qualité des labels | Existe-t-il une vérité terrain, et par qui a-t-elle été produite ? |
| Volumétrie et fraîcheur | Quel volume, et la donnée est-elle actualisée ? |
| Méthode d'extraction | Quelle technique, et à quel coût de maintenance ? |
| Droits d'usage | L'usage envisagé est-il licite ? |
| Limites | Qu'est-ce qui disqualifie ou restreint la source ? |

La qualification ne s'est pas appuyée sur la documentation déclarative des éditeurs. Chaque source a été **sondée par un appel réel** et un échantillon a été conservé dans `docs/samples/`. Le script de sondage des flux RSS, `scripts/probe_feeds.py`, mesure pour chaque flux candidat le taux d'entrées porteuses d'une URL d'image directement lisible dans le XML, le champ dans lequel cette URL se trouve, le taux d'entrées datées et le volume de texte disponible.

### 2.1 Principe directeur : trois rôles, une industrialisation

Le besoin exprimé par le lead technique porte sur **un** script robuste s'exécutant sans intervention, sur une source de mon choix. La stratégie retenue distingue donc deux plans :

- trois sources sont **qualifiées et échantillonnées**, parce qu'elles remplissent trois rôles complémentaires dans la construction du dispositif complet ;
- **une seule est industrialisée**, celle dont les caractéristiques garantissent l'exécution automatisée quotidienne.

| | Source | Rôle dans le dispositif | Format | Industrialisée |
|---|---|---|---|---|
| S1 | RFI, flux RSS | Flux de production continu | XML / RSS 2.0 | **Oui** |
| S2 | FakeNewsNet, PolitiFact | Corpus de référence labellisé | CSV | Non |
| S3 | NewsData.io | API d'agrégation multi-médias | JSON | Non |

---

## 3. Sources qualifiées

### 3.1 S1 : RFI, flux RSS

`https://www.rfi.fr/fr/rss`

| Critère | Constat |
|---|---|
| Rôle | Source d'ingestion continue. Alimente le pipeline quotidien en publications fraîches à soumettre au détecteur. |
| Modalités | Texte (titre et chapô) et image, portés par le même élément `<item>`. L'appariement est structurel, aucune jointure n'est requise. |
| Format brut | XML, RSS 2.0 avec extension Media RSS. URL d'image dans l'attribut `url` de `media:thumbnail`. |
| Langue | Français. |
| Qualité des labels | Aucun label de véracité. Donnée de production destinée à l'inférence, non à l'apprentissage supervisé. |
| Volumétrie et fraîcheur | 23 entrées par appel, fenêtre glissante, mise à jour continue. Pas d'historique accessible. |
| Méthode d'extraction | `feedparser` sur l'URL du flux, puis `requests` pour le téléchargement des images. Ni scraping HTML, ni navigateur automatisé. |
| Droits d'usage | Flux publié par l'éditeur à fin de syndication. Usage de recherche, citation de la source et conservation du lien vers l'article d'origine. Pas de redistribution du contenu intégral. |
| Limites | Chapô et non article complet. Images servies redimensionnées (`w:1024/p:16x9`), pas les originaux. Ligne éditoriale unique, donc biais de source à assumer. |

**Mesures du sondage, 14/08/2026** : 23 entrées sur 23 porteuses d'une image exploitable (100 %), 23 sur 23 datées, 438 caractères de texte en moyenne.

### 3.2 S2 : FakeNewsNet, sous-ensemble PolitiFact

`https://github.com/KaiDMML/FakeNewsNet`

| Critère | Constat |
|---|---|
| Rôle | Corpus de référence labellisé. Fournit la vérité terrain nécessaire à l'entraînement supervisé. |
| Modalités | Titre seul dans le fichier livré. **Ni texte intégral, ni image.** La multimodalité n'est atteinte qu'après collecte des URL référencées. |
| Format brut | CSV, colonnes `id`, `news_url`, `title`, `tweet_ids`. |
| Langue | Anglais. |
| Qualité des labels | Binaire fake / real, annoté par les fact-checkeurs professionnels de PolitiFact. Qualité élevée, méthodologie publiée. |
| Volumétrie et fraîcheur | 432 entrées fake, 624 entrées real, soit 1 056 publications. Corpus figé, collecte arrêtée vers 2020. |
| Méthode d'extraction | Téléchargement direct des CSV. L'enrichissement en texte et en image exigerait la collecte de chaque `news_url` et une clé d'API X/Twitter pour les `tweet_ids`. |
| Droits d'usage | Dépôt public, usage académique et de recherche. |
| Limites | Trois limites cumulées. Corpus périmé, une part significative des `news_url` pointant vers des domaines éteints. Biais politique américain, faible transférabilité au contexte francophone. Multimodalité non fournie, à reconstruire intégralement. |

**Décision** : écartée de l'automatisation. Conservée comme référence méthodologique et comme source d'annotation pour une phase ultérieure.

### 3.3 S3 : NewsData.io

`https://newsdata.io/api/1/latest`

| Critère | Constat |
|---|---|
| Rôle | Agrégateur multi-médias. Apporterait la diversité éditoriale que S1 n'a pas. |
| Modalités | Texte et image dans le même objet JSON, champ `image_url` renseigné et directement exploitable. |
| Format brut | JSON, tableau `results`. |
| Langue | Multilingue, filtrable par `language=fr`. |
| Qualité des labels | Aucun label de véracité. Champs contextuels seulement : `source_name`, `source_priority`, `country`, `category`. |
| Volumétrie et fraîcheur | Temps réel, quota journalier en offre gratuite. |
| Méthode d'extraction | `requests` sur l'endpoint REST, clé d'API en variable d'environnement. |
| Droits d'usage | Conditions d'utilisation de l'éditeur, clé nominative et non partageable. |
| Limites | En offre gratuite, le champ `content` retourne la mention « DISPONIBLE UNIQUEMENT DANS LES FORFAITS PAYANTS ». Seul `description` est utilisable, pour un volume comparable à S1 mais assorti d'un quota et d'une dépendance externe supplémentaire. |

**Décision** : écartée de l'automatisation. Un quota journalier interrompt un pipeline ordonnancé de façon non déterministe, ce qui est incompatible avec la fiabilité attendue d'un flux ETL.

---

## 4. Comparatif et choix de la source industrialisée

Sept flux RSS candidats ont été sondés par un appel réel avant sélection.

| Flux | Langue | Entrées | Texte moyen | Champ image | Taux d'image |
|---|---|---|---|---|---|
| **RFI** | FR | 23 | **438 c.** | `media:thumbnail` | 100 % |
| France 24 | FR | 24 | 268 c. | `media:thumbnail` | 96 % |
| Le Monde | FR | 16 | 234 c. | `media:content` | 100 % |
| franceinfo | FR | 31 | 189 c. | `enclosure` | 100 % |
| 20 Minutes | FR | 30 | 160 c. | `enclosure` | 100 % |
| The Guardian | EN | 45 | 618 c. | `media:content` | 100 % |
| BBC News | EN | 40 | 110 c. | `media:thumbnail` | 100 % |

Le taux d'image ne discrimine pas : six flux sur sept atteignent 100 %. Le critère décisif devient le **volume de texte disponible**, seul déterminant de l'exploitabilité par un modèle de langue en aval.

**Source retenue : RFI.**

Trois arguments :

1. **Volume de texte le plus élevé parmi les flux francophones**, à 438 caractères en moyenne, soit plus du double de franceinfo et près du triple de 20 Minutes. Un titre de 160 caractères ne constitue pas un support de classification, un chapô de 438 caractères commence à en être un.
2. **Complétude structurelle** : 100 % des entrées portent une image et une date de publication, sans exception observée.
3. **Cohérence avec le marché visé.** The Guardian offre un volume de texte supérieur, mais alimenter en anglais un détecteur destiné à un marché francophone introduirait un décalage de distribution entre les données d'entraînement et les données de production.

### 4.1 Source de repli identifiée

franceinfo, `https://www.franceinfo.fr/titres.rss`, présente 31 entrées sur 31 porteuses d'une image en `enclosure`. Structurellement équivalente à S1, moins riche en texte, substituable sans refonte du pipeline en cas d'indisponibilité durable de RFI. **Non implémentée** : le mécanisme de bascule relève d'une évolution ultérieure, non du périmètre courant.

### 4.2 Techniques d'extraction écartées

Scrapy et Selenium ont été écartés. La donnée cible est intégralement disponible dans le XML servi par l'éditeur, ce qui rend l'analyse du HTML de rendu inutile. Recourir au scraping ajouterait une dépendance au balisage éditorial, susceptible de changer sans préavis, un coût de maintenance permanent, et une zone d'incertitude juridique là où la syndication RSS constitue un canal explicitement ouvert par l'éditeur.

---

## 5. Formats retenus pour le traitement et le stockage

Chaque palier de la chaîne appelle un format distinct, choisi pour la contrainte propre à ce palier.

| Palier | Format | Justification |
|---|---|---|
| Atterrissage brut | **JSON Lines** | Écriture par ajout en fin de fichier, un objet par publication. Schéma souple, tolérant à l'apparition de champs optionnels. Une ligne corrompue n'invalide pas le fichier. |
| Données transformées | **CSV** | Représentation colonnaire et typée, directement consommable par pandas et par les bibliothèques d'entraînement. Schéma stabilisé à ce stade. |
| Images | **Fichiers sur disque, référencés par chemin** | Aucun octet binaire ne transite ni en base, ni entre les tâches d'orchestration. L'enregistrement porte un chemin, pas une charge utile. |
| Stockage final | **Table PostgreSQL** | Donnée requêtable, typage fort, unicité garantie par contrainte sur la clé naturelle. Condition de l'idempotence du chargement. |

Le choix de PostgreSQL plutôt que d'une base documentaire tient à la nature de la donnée après transformation : le schéma est stable, les champs sont connus et typés, et l'exigence de non-duplication entre deux exécutions se traite naturellement par une contrainte d'unicité relationnelle.

---

## 6. Sécurité, conformité et gouvernance des données

**Secrets.** Aucun identifiant, clé d'API ou chaîne de connexion n'apparaît dans le code ni dans le dépôt. Les valeurs sensibles sont portées par un fichier `.env` exclu du suivi de version, et injectées à l'exécution. La clé NewsData.io utilisée lors du sondage a été révoquée et régénérée après exposition en clair.

**Accès à la base.** L'instance PostgreSQL est isolée dans le réseau interne du déploiement conteneurisé et n'expose pas de port sur l'hôte au delà du strict nécessaire. L'authentification par mot de passe est active. En contexte de production, le principe du moindre privilège s'appliquerait par la création d'un rôle applicatif restreint aux opérations de lecture et d'écriture sur les tables du pipeline, distinct du rôle propriétaire du schéma.

**Chiffrement.** Le chiffrement des données sensibles au repos n'a pas été mis en œuvre, et cette décision est délibérée. Les données collectées sont des publications de presse **déjà publiques**, dépourvues de donnée à caractère personnel au sens du RGPD hormis le nom de l'auteur, lui-même publié par l'éditeur. Le déclenchement d'un chiffrement applicatif sur des données publiques constituerait un coût sans contrepartie de risque. La question se reposerait si le périmètre s'étendait à des contenus de réseaux sociaux comportant des identifiants d'utilisateurs.

**Droits d'usage.** L'extraction se limite aux flux de syndication publiés par les éditeurs à cette fin. Le contenu intégral des articles n'est ni collecté ni redistribué. Chaque enregistrement conserve l'URL de l'article d'origine et le nom de la source, ce qui préserve l'attribution et permet à tout moment la suppression d'un contenu à la demande de son éditeur.

---

## 7. Limites et suites

- **Biais de source unique.** Un flux éditorial unique transmet sa ligne au corpus. La diversification passerait par l'ajout de flux d'éditeurs distincts, opération de faible coût puisque le pipeline est paramétré par l'URL du flux.
- **Absence de vérité terrain sur le flux industrialisé.** S1 alimente l'inférence, non l'apprentissage supervisé. La constitution d'un corpus étiqueté francophone reste un chantier ouvert, pour lequel S2 fournit une méthodologie d'annotation transposable.
- **Texte partiel.** Le chapô ne remplace pas l'article. Un enrichissement par récupération du corps de l'article supposerait une négociation d'accès avec les éditeurs, hors périmètre technique.
- **Profondeur d'historique nulle.** Un flux RSS ne donne accès qu'à sa fenêtre courante. L'historique se constitue par accumulation, à raison d'une exécution quotidienne du pipeline.

---

## Annexes

- `scripts/probe_feeds.py` : script de sondage des flux candidats.
- `docs/samples/s1_rfi_probe.txt` : sortie du sondage des sept flux.
- `docs/samples/s2_politifact_fake.csv`, `s2_politifact_real.csv` : échantillons FakeNewsNet.
- `docs/samples/s3_newsdata.json` : réponse d'un appel unique à NewsData.io.
- `docs/sources_qualification.md` : grille de qualification détaillée.
