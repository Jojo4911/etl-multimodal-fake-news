"""Extraction des publications multimodales du flux RSS de RFI.

Étape 2 du projet P12. Le module collecte le texte et l'URL de l'image de
chaque publication, puis les serialise en JSON Lines brut.

Hors périmetre de ce module :
- téléchargement des images et gestion des erreurs réseau,
- nettoyage, normalisation et déduplication (pipeline de transformation).
"""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import feedparser

logger = logging.getLogger(__name__)

# Paramètres du module. Ils seront externalisés par la suite
FEED_URL = "https://www.rfi.fr/fr/rss"
SOURCE_NAME = "RFI"
RAW_DIR = Path(__file__).resolve().parents[1] / "data" / "raw"


def fetch_feed(feed_url: str = FEED_URL) -> feedparser.FeedParserDict:
    """Recupère et parse le flux RSS, et retourne l'objet feedparser brut."""
    logger.info("Recuperation du flux : %s", feed_url)
    feed = feedparser.parse(feed_url)
    if feed.bozo:
        logger.warning("Anomalie signalée par feedparser : %s", feed.bozo_exception)
    logger.info("%d entrées retournées par le flux", len(feed.entries))
    return feed


def extract_image_url(entry: feedparser.FeedParserDict) -> str | None:
    """Retourne l'URL de l'image associée à une entrée, ou None.

    RFI expose l'image dans `media_thumbnail`, qui est une liste de dicts
    dont la clé utile est "url" et non "href". Les deux replis couvrent les
    flux qui utilisent `media_content` ou une enclosure classique.
    """
    for thumbnail in entry.get("media_thumbnail") or []:
        url = thumbnail.get("url")
        if url:
            return url

    for media in entry.get("media_content") or []:
        url = media.get("url")
        if url:
            return url

    for link in entry.get("links") or []:
        if link.get("rel") == "enclosure" and str(link.get("type", "")).startswith("image/"):
            return link.get("href")

    return None


def build_publication_id(article_url: str) -> str:
    """Identifiant stable dérivé de l'URL de l'article.

    Sert de clé naturelle pour le nom de fichier image et pour la
    déduplication.
    """
    return hashlib.sha1(article_url.encode("utf-8")).hexdigest()[:16]


def parse_entries(
    feed: feedparser.FeedParserDict, source: str = SOURCE_NAME
) -> list[dict]:
    """Convertit les entrées du flux en enregistrements de publication.

    Les valeurs sont conservées telles quelles : la normalisation des dates
    et le nettoyage du texte appartiennent au pipeline de transformation.
    """
    fetched_at = datetime.now(timezone.utc).isoformat()
    publications: list[dict] = []

    for entry in feed.entries:
        article_url = entry.get("link")
        if not article_url:
            logger.warning("Entrée sans URL d'article, ignorée : %s", entry.get("title"))
            continue

        publications.append(
            {
                "id": build_publication_id(article_url),
                "source": source,
                "article_url": article_url,
                "title": entry.get("title"),
                "text": entry.get("summary"),
                "published": entry.get("published"),
                "image_url": extract_image_url(entry),
                "fetched_at": fetched_at,
            }
        )

    without_image = sum(1 for pub in publications if not pub["image_url"])
    logger.info(
        "%d publications parsées, dont %d sans URL d'image",
        len(publications),
        without_image,
    )
    return publications


def save_raw(publications: list[dict], raw_dir: Path = RAW_DIR) -> Path:
    """Ecrit les publications en JSON Lines horodaté et retourne le chemin."""
    raw_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_path = raw_dir / f"{SOURCE_NAME.lower()}_{stamp}.jsonl"

    with output_path.open("w", encoding="utf-8", newline="\n") as handle:
        for publication in publications:
            handle.write(json.dumps(publication, ensure_ascii=False) + "\n")

    logger.info("%d publications ecrites dans %s", len(publications), output_path)
    return output_path


def run(feed_url: str = FEED_URL, raw_dir: Path = RAW_DIR) -> Path:
    """Enchaine les quatre étapes d'extraction et retourne le fichier produit."""
    feed = fetch_feed(feed_url)
    publications = parse_entries(feed)
    return save_raw(publications, raw_dir)


if __name__ == "__main__":
    # Configuration provisoire, remplacée par le logging externalisé.
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    run()