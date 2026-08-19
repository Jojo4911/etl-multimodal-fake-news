"""Extraction des publications multimodales du flux RSS de RFI.

Étape 2 du projet P12. Le module récupère le flux, en extrait le texte et
l'URL de l'image de chaque publication, télécharge les images sur disque,
puis sérialise les enregistrements en JSON Lines brut.

Le script s'execute sans aucune intervention manuelle :
    uv run python src/extract_rss.py

Hors perimètre : nettoyage, normalisation et déduplication, qui relevent du
pipeline de transformation.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import feedparser
import requests

logger = logging.getLogger("extract_rss")

# Valeurs par defaut, toutes surchargeables en ligne de commande.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_FEED_URL = "https://www.rfi.fr/fr/rss"
DEFAULT_SOURCE_NAME = "RFI"
DEFAULT_RAW_DIR = PROJECT_ROOT / "data" / "raw"
DEFAULT_IMAGES_DIR = PROJECT_ROOT / "data" / "images"
DEFAULT_LOG_DIR = PROJECT_ROOT / "logs"
DEFAULT_TIMEOUT = 10.0
DEFAULT_MAX_ENTRIES = 0  # 0 signifie aucune limite
USER_AGENT = "P12-ETL-Multimodal/1.0 (projet pedagogique OpenClassrooms)"
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp", ".gif"}


def configure_logging(log_dir: Path, verbose: bool = False) -> None:
    """Configure la sortie console et le fichier de log horodaté."""
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"extract_rss_{datetime.now():%Y%m%d}.log"

    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_path, encoding="utf-8"),
        ],
    )
    logger.info("Journalisation active, fichier : %s", log_path)


def fetch_feed(feed_url: str, timeout: float = DEFAULT_TIMEOUT) -> feedparser.FeedParserDict | None:
    """Récupère et parse le flux RSS, ou retourne None en cas d'echec reseau.

    Le transport passe par `requests` et non par feedparser, qui n'expose
    aucun controle de timeout : un flux muet figerait le script.
    """
    logger.info("Récuperation du flux : %s", feed_url)
    try:
        response = requests.get(
            feed_url, timeout=timeout, headers={"User-Agent": USER_AGENT}
        )
        response.raise_for_status()
    except requests.Timeout:
        logger.error("Timeout de %.1f s depasse sur le flux %s", timeout, feed_url)
        return None
    except requests.RequestException as exc:
        logger.error("Echec de récuperation du flux %s : %s", feed_url, exc)
        return None

    feed = feedparser.parse(response.content)
    if feed.bozo:
        logger.warning("Anomalie signalée par feedparser : %s", feed.bozo_exception)
    logger.info("%d entrées retournées par le flux", len(feed.entries))
    return feed


def extract_image_url(entry: feedparser.FeedParserDict) -> str | None:
    """Retourne l'URL de l'image associée a une entrée, ou None.

    RFI expose l'image dans `media_thumbnail`, liste de dicts dont la clé
    utile est "url" et non "href". Les replis couvrent `media_content` et
    l'enclosure classique.
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
    """Identifiant stable derive de l'URL de l'article.

    Sert de clé naturelle pour le nom de fichier image et pour la
    déduplication du pipeline de transformation.
    """
    return hashlib.sha1(article_url.encode("utf-8")).hexdigest()[:16]


def parse_entries(
    feed: feedparser.FeedParserDict,
    source: str = DEFAULT_SOURCE_NAME,
    max_entries: int = DEFAULT_MAX_ENTRIES,
) -> list[dict]:
    """Convertit les entrées du flux en enregistrements de publication.

    Les valeurs sont conservées telles quelles : la normalisation des dates
    et le nettoyage du texte appartiennent au pipeline de transformation.
    """
    fetched_at = datetime.now(timezone.utc).isoformat()
    entries = feed.entries[:max_entries] if max_entries > 0 else feed.entries
    publications: list[dict] = []

    for entry in entries:
        article_url = entry.get("link")
        if not article_url:
            logger.warning("Entree sans URL d'article, ignoree : %s", entry.get("title"))
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
                "image_path": None,
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


def _image_extension(image_url: str) -> str:
    """Deduit l'extension de fichier a partir de l'URL, avec repli sur .jpg."""
    suffix = Path(urlparse(image_url).path).suffix.lower()
    return suffix if suffix in IMAGE_EXTENSIONS else ".jpg"


def download_image(
    image_url: str,
    destination: Path,
    timeout: float = DEFAULT_TIMEOUT,
) -> bool:
    """Télécharge une image vers `destination`, et retourne le succes.

    Un fichier déjà present n'est pas retéléchargé : le nom dérive de
    l'identifiant de publication, donc le contenu est le même.
    """
    if destination.exists():
        logger.debug("Image déjà présente, téléchargement ignoré : %s", destination.name)
        return True

    try:
        response = requests.get(
            image_url, timeout=timeout, headers={"User-Agent": USER_AGENT}
        )
        response.raise_for_status()
    except requests.Timeout:
        logger.warning("Timeout de %.1f s sur l'image %s", timeout, image_url)
        return False
    except requests.RequestException as exc:
        logger.warning("Echec de téléchargement de l'image %s : %s", image_url, exc)
        return False

    try:
        destination.write_bytes(response.content)
    except OSError as exc:
        logger.error("Ecriture impossible pour %s : %s", destination, exc)
        return False

    logger.debug("Image telechargée : %s (%d octets)", destination.name, len(response.content))
    return True


def download_images(
    publications: list[dict],
    images_dir: Path,
    timeout: float = DEFAULT_TIMEOUT,
) -> list[dict]:
    """Télécharge les images des publications et renseigne `image_path`.

    Une image manquante ou en echec n'interrompt pas le traitement : la
    publication est conservée avec `image_path` a None.
    """
    images_dir.mkdir(parents=True, exist_ok=True)
    downloaded = 0

    for publication in publications:
        image_url = publication["image_url"]
        if not image_url:
            continue

        destination = images_dir / f"{publication['id']}{_image_extension(image_url)}"
        if download_image(image_url, destination, timeout):
            publication["image_path"] = str(destination.relative_to(PROJECT_ROOT).as_posix())
            downloaded += 1

    logger.info(
        "%d images disponibles sur %d publications", downloaded, len(publications)
    )
    return publications


def save_raw(
    publications: list[dict],
    raw_dir: Path,
    source: str = DEFAULT_SOURCE_NAME,
) -> Path:
    """Ecrit les publications en JSON Lines horodaté et retourne le chemin."""
    raw_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_path = raw_dir / f"{source.lower()}_{stamp}.jsonl"

    with output_path.open("w", encoding="utf-8", newline="\n") as handle:
        for publication in publications:
            handle.write(json.dumps(publication, ensure_ascii=False) + "\n")

    logger.info("%d publications écrites dans %s", len(publications), output_path)
    return output_path


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Déclare les paramètres configurables du script."""
    parser = argparse.ArgumentParser(
        description="Extraction multimodale d'un flux RSS vers JSON Lines."
    )
    parser.add_argument("--feed-url", default=DEFAULT_FEED_URL, help="URL du flux RSS")
    parser.add_argument("--source", default=DEFAULT_SOURCE_NAME, help="Nom de la source")
    parser.add_argument("--raw-dir", type=Path, default=DEFAULT_RAW_DIR)
    parser.add_argument("--images-dir", type=Path, default=DEFAULT_IMAGES_DIR)
    parser.add_argument("--log-dir", type=Path, default=DEFAULT_LOG_DIR)
    parser.add_argument(
        "--max-entries", type=int, default=DEFAULT_MAX_ENTRIES,
        help="Nombre maximal d'entrées, 0 pour aucune limite",
    )
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT)
    parser.add_argument("--verbose", action="store_true", help="Passe les logs en DEBUG")
    return parser.parse_args(argv)


def run(
    feed_url: str = DEFAULT_FEED_URL,
    source: str = DEFAULT_SOURCE_NAME,
    raw_dir: Path = DEFAULT_RAW_DIR,
    images_dir: Path = DEFAULT_IMAGES_DIR,
    max_entries: int = DEFAULT_MAX_ENTRIES,
    timeout: float = DEFAULT_TIMEOUT,
) -> Path | None:
    """Enchaine les étapes d'extraction et retourne le fichier produit.

    Retourne None si le flux est injoignable. Point d'entrée unique reutilisé
    tel quel par la tache `extract` du DAG (T10).
    """
    feed = fetch_feed(feed_url, timeout)
    if feed is None:
        logger.error("Extraction interrompue : flux indisponible")
        return None

    publications = parse_entries(feed, source, max_entries)
    if not publications:
        logger.error("Extraction interrompue : aucune publication exploitable")
        return None

    publications = download_images(publications, images_dir, timeout)
    return save_raw(publications, raw_dir, source)


def main() -> int:
    args = parse_args()
    configure_logging(args.log_dir, args.verbose)
    output_path = run(
        feed_url=args.feed_url,
        source=args.source,
        raw_dir=args.raw_dir,
        images_dir=args.images_dir,
        max_entries=args.max_entries,
        timeout=args.timeout,
    )
    if output_path is None:
        return 1
    logger.info("Extraction terminée : %s", output_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())