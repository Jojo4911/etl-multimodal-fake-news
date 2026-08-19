"""Pipeline de transformation des publications extraites en JSON Lines.

Trois étapes explicites : lecture, traitement, export.

Entrée  : un fichier data/raw/rfi_<horodatage>.jsonl produit par extract_rss.py.
Sortie  : un fichier data/processed/publications_<horodatage>.csv.

Le chemin d'entrée est passé en paramètre, jamais deviné depuis le contenu du
dossier data/raw/. run() retourne le chemin du CSV produit : c'est cette valeur
qui transitera par XCom, jamais le contenu.
"""

from __future__ import annotations

import argparse
import csv
import html
import json
import logging
import re
import sys
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path

logger = logging.getLogger(__name__)

# Colonnes du CSV de sortie, dans l'ordre.
CSV_FIELDS = [
    "id",
    "source",
    "article_url",
    "title",
    "text_clean",
    "published_at",
    "image_path",
    "image_valid",
    "fetched_at",
]

BALISE_HTML = re.compile(r"<[^>]+>")
ESPACES_MULTIPLES = re.compile(r"\s+")


# --------------------------------------------------------------------------
# Journalisation
# --------------------------------------------------------------------------

def configure_logging(log_dir: Path, verbose: bool = False) -> None:
    """Configure la journalisation fichier + console."""
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"transform_{datetime.now():%Y%m%d}.log"

    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )


# --------------------------------------------------------------------------
# Etape 1 : lecture
# --------------------------------------------------------------------------

def lire_jsonl(chemin: Path) -> list[dict]:
    """Lit un fichier JSON Lines et retourne la liste des enregistrements.

    Une ligne illisible est journalisée et ignorée, elle n'interrompt pas
    le pipeline.
    """
    if not chemin.is_file():
        raise FileNotFoundError(f"Fichier d'entrée introuvable : {chemin}")

    enregistrements: list[dict] = []
    lignes_ko = 0

    with chemin.open("r", encoding="utf-8") as f:
        for numero, ligne in enumerate(f, start=1):
            ligne = ligne.strip()
            if not ligne:
                continue
            try:
                enregistrements.append(json.loads(ligne))
            except json.JSONDecodeError as exc:
                lignes_ko += 1
                logger.warning("Ligne %d illisible, ignorée : %s", numero, exc)

    logger.info(
        "LECTURE | fichier=%s | lignes valides=%d | lignes ignorées=%d",
        chemin.name, len(enregistrements), lignes_ko,
    )
    return enregistrements


# --------------------------------------------------------------------------
# Etape 2 : traitement
# --------------------------------------------------------------------------

def nettoie_texte(brut: str | None) -> str:
    """Retire les balises HTML, décode les entités, normalise les espaces."""
    if not brut:
        return ""
    sans_balises = BALISE_HTML.sub(" ", brut)
    decode = html.unescape(sans_balises)
    return ESPACES_MULTIPLES.sub(" ", decode).strip()


def normalise_date(brut: str | None) -> str:
    """Convertit une date RFC 822 en ISO 8601 UTC.

    Retourne une chaine vide si la date est absente ou illisible.
    """
    if not brut:
        return ""
    try:
        dt = parsedate_to_datetime(brut)
    except (TypeError, ValueError) as exc:
        logger.warning("Date illisible, champ laissé vide : %r (%s)", brut, exc)
        return ""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def valide_image(chemin: str | None, taille_min: int) -> bool:
    """Verifie qu'un fichier image existe sur disque et n'est pas un leurre.

    Un fichier de quelques octets est en géneral une page d'erreur ou un
    pixel de suivi, pas une illustration exploitable.
    """
    if not chemin:
        return False
    p = Path(chemin)
    if not p.is_file():
        logger.debug("Image absente du disque : %s", chemin)
        return False
    if p.stat().st_size < taille_min:
        logger.debug("Image trop petite (%d octets) : %s", p.stat().st_size, chemin)
        return False
    return True


def deduplique(enregistrements: list[dict], cle: str = "id") -> list[dict]:
    """Supprime les doublons sur la clé naturelle, en gardant la derniere
    occurrence rencontrée (la plus fraiche dans un flux chronologique)."""
    avant = len(enregistrements)
    index: dict[str, dict] = {}
    for rec in enregistrements:
        valeur = rec.get(cle)
        if not valeur:
            logger.warning("Enregistrement sans clé %r, ignoré", cle)
            continue
        index[valeur] = rec
    apres = len(index)
    logger.info(
        "DEDOUBLONNAGE | avant=%d | apres=%d | doublons retirés=%d",
        avant, apres, avant - apres,
    )
    return list(index.values())


def transforme(enregistrements: list[dict], taille_min_image: int) -> list[dict]:
    """Applique nettoyage, normalisation de date et validation d'image."""
    sortie: list[dict] = []
    dates_ko = 0
    images_ok = 0

    for rec in enregistrements:
        published_at = normalise_date(rec.get("published"))
        if not published_at:
            dates_ko += 1

        image_path = rec.get("image_path") or ""
        image_valid = valide_image(image_path, taille_min_image)
        if image_valid:
            images_ok += 1

        sortie.append({
            "id": rec.get("id", ""),
            "source": rec.get("source", ""),
            "article_url": rec.get("article_url", ""),
            "title": nettoie_texte(rec.get("title")),
            "text_clean": nettoie_texte(rec.get("text")),
            "published_at": published_at,
            "image_path": image_path,
            "image_valid": image_valid,
            "fetched_at": rec.get("fetched_at", ""),
        })

    total = len(sortie)
    logger.info(
        "NETTOYAGE TEXTE | entrées=%d | sorties=%d", len(enregistrements), total
    )
    logger.info(
        "NORMALISATION DATES | entrées=%d | normalisées=%d | illisibles=%d",
        total, total - dates_ko, dates_ko,
    )
    logger.info(
        "VALIDATION IMAGES | entrées=%d | exploitables=%d | non exploitables=%d",
        total, images_ok, total - images_ok,
    )
    return sortie


def filtre_texte_court(enregistrements: list[dict], min_caracteres: int) -> list[dict]:
    """Ecarte les publications dont le texte nettoyé est trop court pour un
    traitement NLP."""
    avant = len(enregistrements)
    retenus = [r for r in enregistrements if len(r["text_clean"]) >= min_caracteres]
    apres = len(retenus)
    logger.info(
        "FILTRE TEXTE COURT | seuil=%d car. | avant=%d | après=%d | écartés=%d",
        min_caracteres, avant, apres, avant - apres,
    )
    return retenus


# --------------------------------------------------------------------------
# Etape 3 : export
# --------------------------------------------------------------------------

def exporte_csv(enregistrements: list[dict], dossier_sortie: Path, source: str) -> Path:
    """Ecrit le CSV final et retourne son chemin."""
    dossier_sortie.mkdir(parents=True, exist_ok=True)
    horodatage = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    chemin = dossier_sortie / f"{source}_{horodatage}.csv"

    with chemin.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(enregistrements)

    logger.info("EXPORT | lignes=%d | fichier=%s", len(enregistrements), chemin)
    return chemin


# --------------------------------------------------------------------------
# Orchestration
# --------------------------------------------------------------------------

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Transforme un JSON Lines brut en CSV exploitable."
    )
    parser.add_argument("--input", required=True,
                        help="Chemin du fichier .jsonl à transformer")
    parser.add_argument("--out-dir", default="data/processed",
                        help="Dossier de sortie du CSV")
    parser.add_argument("--log-dir", default="logs",
                        help="Dossier des journaux")
    parser.add_argument("--source", default="rfi",
                        help="Prefixe du fichier de sortie")
    parser.add_argument("--min-text-chars", type=int, default=50,
                        help="Longueur minimale du texte nettoyé")
    parser.add_argument("--min-image-bytes", type=int, default=1024,
                        help="Taille minimale d'une image pour être exploitable")
    parser.add_argument("--verbose", action="store_true",
                        help="Journalisation en niveau DEBUG")
    return parser.parse_args(argv)


def run(
    input_path: str,
    out_dir: str = "data/processed",
    source: str = "rfi",
    min_text_chars: int = 50,
    min_image_bytes: int = 1024,
) -> str:
    """Execute le pipeline complet et retourne le chemin du CSV produit.

    C'est cette valeur de retour qui sera poussée dans XCom par la tâche
    transform du DAG.
    """
    logger.info("=== Debut de la transformation ===")

    bruts = lire_jsonl(Path(input_path))
    dedupliques = deduplique(bruts)
    transformes = transforme(dedupliques, min_image_bytes)
    retenus = filtre_texte_court(transformes, min_text_chars)
    chemin_csv = exporte_csv(retenus, Path(out_dir), source)

    logger.info(
        "=== Fin de la transformation | %d entrées brutes -> %d lignes exportées ===",
        len(bruts), len(retenus),
    )
    return str(chemin_csv)


def main() -> int:
    args = parse_args()
    configure_logging(Path(args.log_dir), verbose=args.verbose)
    try:
        run(
            input_path=args.input,
            out_dir=args.out_dir,
            source=args.source,
            min_text_chars=args.min_text_chars,
            min_image_bytes=args.min_image_bytes,
        )
    except FileNotFoundError as exc:
        logger.error("Fichier introuvable : %s", exc)
        return 1
    except OSError as exc:
        logger.error("Erreur d'accès disque : %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())