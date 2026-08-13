"""Sondage de flux RSS : où sont les images, et dans quelle proportion.
Code jetable de qualification de sources. Ne pas polir, ne pas réutiliser.
"""

import re
from collections import Counter

import feedparser

UA = "Mozilla/5.0 (compatible; P12-probe/0.1)"
IMG_HTML = re.compile(r'<img[^>]+src=["\']([^"\']+)', re.I)

FLUX = [
    "https://www.franceinfo.fr/titres.rss",
    "https://www.france24.com/fr/rss",
    "https://www.20minutes.fr/feeds/rss-une.xml",
    "https://www.lemonde.fr/rss/une.xml",
    "https://www.rfi.fr/fr/rss",
    "https://feeds.bbci.co.uk/news/rss.xml",
    "https://www.theguardian.com/world/rss",
]


def trouve_image(entry):
    """Retourne (url, champ_source). Les canaux XML d'abord, le HTML en dernier."""
    for media in entry.get("media_content", []):
        url = media.get("url")
        mime = media.get("type", "")
        medium = media.get("medium", "")
        if url and medium in ("", "image") and not mime.startswith(("video", "audio")):
            return url, "media:content"

    for thumb in entry.get("media_thumbnail", []):
        if thumb.get("url"):
            return thumb["url"], "media:thumbnail"

    for enc in entry.get("enclosures", []):
        if enc.get("type", "").startswith("image/") and enc.get("href"):
            return enc["href"], "enclosure"

    for link in entry.get("links", []):
        if link.get("rel") == "enclosure" and link.get("type", "").startswith("image/"):
            return link.get("href"), "link[enclosure]"

    trouve = IMG_HTML.search(entry.get("summary", ""))
    if trouve:
        return trouve.group(1), "html:summary (DEGRADE)"

    return None, "aucun"


def texte_brut(entry):
    """Longueur du texte disponible dans le flux, balises retirées."""
    brut = entry.get("summary", "") or entry.get("title", "")
    return len(re.sub(r"<[^>]+>", " ", brut).strip())


for url_flux in FLUX:
    d = feedparser.parse(url_flux, agent=UA)
    n = len(d.entries)

    print("=" * 70)
    print(f"Flux    : {url_flux}")
    print(f"HTTP    : {getattr(d, 'status', 'n/a')}   bozo : {d.bozo}")
    if d.bozo:
        print(f"  erreur parsing : {d.bozo_exception}")
    print(f"Entrees : {n}")

    if n == 0:
        continue

    champs = Counter()
    exemple = None
    avec_date = 0
    longueurs = []

    for entry in d.entries:
        img, champ = trouve_image(entry)
        champs[champ] += 1
        if img and exemple is None:
            exemple = (champ, img)
        if entry.get("published_parsed"):
            avec_date += 1
        longueurs.append(texte_brut(entry))

    exploitables = n - champs["aucun"] - champs["html:summary (DEGRADE)"]
    print(f"Images XML exploitables : {exploitables}/{n} ({100 * exploitables / n:.0f} %)")
    print(f"Dates de publication    : {avec_date}/{n}")
    print(f"Texte moyen             : {sum(longueurs) // n} caracteres")
    print("Repartition des champs  :")
    for champ, nb in champs.most_common():
        print(f"    {champ:<28} {nb}")
    if exemple:
        print(f"Exemple ({exemple[0]}) : {exemple[1][:110]}")