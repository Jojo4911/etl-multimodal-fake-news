# Qualification des sources : projet 12, détecteur de fake news multimodal

Document de travail. Matière première du rapport d'exploration (livrable 1).
Sondage réalisé le 13/08/2026, échantillons dans `docs/samples/`.

## Principe de sélection

Trois sources, trois rôles complémentaires. Une seule est industrialisée : le pipeline automatisé cible un flux de production, les deux autres alimentent la constitution du corpus d'entraînement et la validation.

|    | Source                  | Rôle                          | Format      | Industrialisée |
|----|-------------------------|-------------------------------|-------------|----------------|
| S1 | RFI, flux RSS           | Flux de production continu    | XML/RSS 2.0 | **Oui**        |
| S2 | FakeNewsNet, PolitiFact | Corpus de référence labellisé | CSV         | Non            |
| S3 | NewsData.io             | API d'agrégation multi-médias | JSON        | Non            |

## S1 : RFI, flux RSS

- **Rôle** : source d'ingestion continue. Alimente le pipeline quotidien en publications fraîches à soumettre au détecteur.
- **Modalités** : texte (titre + chapô) et image, liés dans la même entrée `<item>`. L'association texte-image est structurelle, aucune jointure requise.
- **Format brut** : XML, RSS 2.0 avec extension Media RSS. Image en `media:thumbnail`, attribut `url`.
- **Langue** : français.
- **Qualité des labels** : aucun label de véracité. Données de production destinées à l'inférence, pas à l'apprentissage supervisé.
- **Volumétrie et fraîcheur** : 23 entrées par appel, glissantes, mise à jour continue. Rejouable quotidiennement, pas d'historique accessible.
- **Méthode d'extraction** : `feedparser` sur l'URL du flux, puis `requests` pour le téléchargement des images. Ni scraping HTML, ni navigateur headless.
- **Droits d'usage** : flux publié par l'éditeur pour la syndication. Usage de recherche, citation de la source et conservation du lien vers l'article d'origine. Pas de redistribution du contenu intégral.
- **Limites** : chapô de 438 caractères en moyenne, pas l'article complet. Images servies redimensionnées (`w:1024/p:16x9`), pas les originaux. Ligne éditoriale unique, donc biais de source à assumer.
- **Mesures du sondage** : 23/23 entrées avec image exploitable (100 %), 23/23 avec date de publication, 438 caractères de texte en moyenne.

## S2 : FakeNewsNet, sous-ensemble PolitiFact

- **Rôle** : corpus de référence labellisé. Fournit la vérité terrain nécessaire à l'entraînement supervisé du détecteur.
- **Modalités** : titre uniquement dans le CSV livré. **Ni texte intégral, ni image.** Le caractère multimodal n'est atteint qu'après crawl des URL.
- **Format brut** : CSV, colonnes `id`, `news_url`, `title`, `tweet_ids`.
- **Langue** : anglais.
- **Qualité des labels** : binaire fake/real, annoté par les fact-checkeurs professionnels de PolitiFact. Qualité élevée, méthode documentée.
- **Volumétrie et fraîcheur** : 432 entrées fake, 624 entrées real. Corpus figé, collecte arrêtée vers 2020.
- **Méthode d'extraction** : téléchargement direct des CSV depuis GitHub. L'enrichissement texte + image exigerait un crawl de chaque `news_url` et une clé d'API X/Twitter pour les `tweet_ids`.
- **Droits d'usage** : dépôt public, usage académique et de recherche.
- **Limites** : trois limites rédhibitoires pour l'industrialisation. Corpus périmé, une part significative des `news_url` pointant vers des domaines éteints. Biais politique américain, faible transférabilité au contexte francophone. Multimodalité non fournie, à reconstruire par crawl.
- **Décision** : écartée de l'automatisation. Conservée comme référence méthodologique et source d'annotation pour une phase ultérieure.

## S3 : NewsData.io

- **Rôle** : agrégateur multi-médias. Offrirait la diversité éditoriale que S1 n'a pas.
- **Modalités** : texte et image dans le même objet JSON, champ `image_url` renseigné et directement exploitable.
- **Format brut** : JSON, un tableau `results`.
- **Langue** : multilingue, filtrable par `language=fr`.
- **Qualité des labels** : aucun label de véracité. Champs contextuels seulement (`source_name`, `source_priority`, `country`, `category`).
- **Volumétrie et fraîcheur** : temps réel, quota journalier en offre gratuite.
- **Méthode d'extraction** : `requests` sur l'endpoint REST, clé d'API en variable d'environnement.
- **Droits d'usage** : conditions d'utilisation de l'éditeur, clé nominative.
- **Limites** : en offre gratuite, le champ `content` retourne « DISPONIBLE UNIQUEMENT DANS LES FORFAITS PAYANTS ». Seul le champ `description` est utilisable, soit un volume comparable à S1 mais assorti d'un quota et d'un point de défaillance supplémentaire.
- **Décision** : écartée de l'automatisation. Un quota journalier interrompt un pipeline ordonnancé de façon non déterministe, ce qui est incompatible avec la démonstration d'un flux ETL fiable.

## Source de repli identifiée

franceinfo (`https://www.franceinfo.fr/titres.rss`) : 31/31 entrées avec image en `enclosure`, 189 caractères de texte moyen. Structurellement équivalente à S1, moins riche en texte. Substituable sans modification du pipeline en cas d'indisponibilité de RFI. Non implémentée.

## Formats retenus le long de la chaîne

| Palier               | Format                                     | Justification                                     |
|----------------------|--------------------------------------------|---------------------------------------------------|
| Atterrissage brut    | JSON Lines                                 | Ajout en fin de fichier, schéma souple, rejouable |
| Données transformées | CSV                                        | Colonnaire, typé, consommable par pandas          |
| Images               | Fichiers sur disque, référencés par chemin | Aucun octet en base ni en XCom                    |
| Stockage final       | Table PostgreSQL                           | Requêtable, unicité garantie par clé naturelle    |