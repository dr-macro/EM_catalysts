import feedparser
import pandas as pd
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import quote
import os
from newspaper import Article
# === CONFIG ===
scrape_content = False  # Set to True if you want full article scraping
today = datetime.today().strftime("%Y-%m-%d")
csv_path = f"data/articles_{today}.csv"

# === RSS Feed List ===
rss_feeds = {
    "MarketWatch Top": "https://www.marketwatch.com/rss/topstories",
    "CNBC Top News": "https://www.cnbc.com/id/100003114/device/rss/rss.html",
    "Investing.com All News": "https://uk.investing.com/rss/news.rss",
    "Investing.com Bond News": "https://uk.investing.com/rss/bonds.rss",
    "Google News Reuters" : "https://news.google.com/rss/search?q=site%3Areuters.com&hl=en-US&gl=US&ceid=US%3Aen",
    "Google News WSJ" : "https://news.google.com/rss/search?q=site:wsj.com&hl=en-US&gl=US&ceid=US:en",
    "Google News FT" : "https://news.google.com/rss/search?q=site:ft.com&hl=en-US&gl=US&ceid=US:en",
}

# === Local press (Europe, EM, APAC), written to data/localpress_<date>.csv, NOT to articles_<date>.csv ===
# Kept apart so the daily pipeline and every reader of articles_*.csv see the same corpus as before; the inflection
# work merges the two with salience pooled by region (HARNESS_PLAN.md 4b). Titles stay in their own language (fr, de,
# it, es, pt); translation happens downstream, not here. Each feed was checked on 2026-10-09 (items within 2 days in
# brackets).
LOCAL_EVERY_H = 3            # local feeds are fetched at most every 3 hours (Google News query volume from CI)
LOCAL_MAX_AGE_D = 3          # drop items published more than 3 days ago (search feeds return weeks-old items)
_LOC = {"en": ("en-US", "US", "US:en"), "en-GB": ("en-GB", "GB", "GB:en"),
        "es-MX": ("es-419", "MX", "MX:es-419"), "es-CO": ("es-419", "CO", "CO:es-419"), "es-CL": ("es-419", "CL", "CL:es-419"),
        "pt-BR": ("pt-BR", "BR", "BR:pt-419"), "fr": ("fr", "FR", "FR:fr"), "de": ("de", "DE", "DE:de"),
        "de-CH": ("de", "CH", "CH:de"), "de-AT": ("de", "AT", "AT:de"), "it": ("it", "IT", "IT:it"),
        "es-ES": ("es", "ES", "ES:es"), "pt-PT": ("pt-PT", "PT", "PT:pt-150")}
# Keyword filters by language; a locale with its own entry (es-ES, pt-PT) overrides the language's.
_MACRO = {
    "en": '(economy OR inflation OR "central bank" OR rates OR bond OR budget OR currency OR government OR election OR minister OR tariff OR GDP)',
    "es": '(economía OR inflación OR "banco central" OR tasas OR bonos OR presupuesto OR peso OR gobierno OR elecciones OR ministro OR PIB OR fiscal)',
    "pt": '(economia OR inflação OR "banco central" OR juros OR Copom OR fiscal OR orçamento OR governo OR eleição OR câmbio OR dólar)',
    "fr": '(économie OR inflation OR BCE OR taux OR obligations OR budget OR déficit OR dette OR gouvernement OR élection OR ministre OR croissance OR OAT)',
    "de": '(Wirtschaft OR Inflation OR EZB OR Zinsen OR Anleihen OR Haushalt OR Schulden OR Regierung OR Wahl OR Minister OR Konjunktur OR Bundesbank)',
    "it": '(economia OR inflazione OR BCE OR tassi OR BTP OR spread OR bilancio OR manovra OR debito OR governo OR elezioni OR ministro OR PIL)',
    "es-ES": '(economía OR inflación OR BCE OR tipos OR bonos OR "prima de riesgo" OR presupuestos OR deuda OR Gobierno OR elecciones OR ministro OR PIB)',
    "pt-PT": '(economia OR inflação OR BCE OR juros OR dívida OR orçamento OR governo OR eleições OR ministro OR PIB)',
}


def _gn(domain, loc="en", macro=False):
    """Google News search feed for one site, optionally restricted to macro / politics words in the feed's language."""
    hl, gl, ceid = _LOC[loc]
    q = f"site:{domain}" + (f" {_MACRO.get(loc, _MACRO[loc[:2]])}" if macro else "")
    return f"https://news.google.com/rss/search?q={quote(q)}&hl={hl}&gl={gl}&ceid={quote(ceid)}"


# name: (url, market, region, language)
local_feeds = {
    # Europe (euro area, UK, Switzerland)
    "Politico Europe": (_gn("politico.eu", macro=True), "EU", "Europe", "en"),                       # [27]
    "Les Echos": (_gn("lesechos.fr", "fr", macro=True), "FR", "Europe", "fr"),                       # [66]
    "Le Figaro Eco": (_gn("lefigaro.fr", "fr", macro=True), "FR", "Europe", "fr"),                   # [60]
    "Le Monde Economie": ("https://www.lemonde.fr/economie/rss_full.xml", "FR", "Europe", "fr"),     # [20]
    "Handelsblatt": (_gn("handelsblatt.com", "de", macro=True), "DE", "Europe", "de"),               # [44]
    "FAZ Wirtschaft": (_gn("faz.net/aktuell/wirtschaft", "de"), "DE", "Europe", "de"),               # [50]
    "Tagesschau Wirtschaft": ("https://www.tagesschau.de/wirtschaft/index~rss2.xml", "DE", "Europe", "de"),  # [12]
    "Il Sole 24 Ore": (_gn("ilsole24ore.com", "it", macro=True), "IT", "Europe", "it"),              # [81]
    "Milano Finanza": (_gn("milanofinanza.it", "it", macro=True), "IT", "Europe", "it"),             # [77]
    "BBC Business": ("https://feeds.bbci.co.uk/news/business/rss.xml", "GB", "Europe", "en"),       # [36]
    "Guardian Business": ("https://www.theguardian.com/uk/business/rss", "GB", "Europe", "en"),     # [27]
    "Telegraph Business": (_gn("telegraph.co.uk/business", "en-GB"), "GB", "Europe", "en"),          # [21]
    "Expansion": (_gn("expansion.com", "es-ES", macro=True), "ES", "Europe", "es"),                  # [26]
    "Cinco Dias": (_gn("cincodias.elpais.com", "es-ES", macro=True), "ES", "Europe", "es"),          # [30]
    "NZZ": (_gn("nzz.ch", "de-CH", macro=True), "CH", "Europe", "de"),                               # [37]
    "Swissinfo": (_gn("swissinfo.ch/eng"), "CH", "Europe", "en"),                                    # [22]
    "DutchNews": (_gn("dutchnews.nl"), "NL", "Europe", "en"),                                        # [19]
    "Brussels Times": (_gn("brusselstimes.com", macro=True), "BE", "Europe", "en"),                  # [19]
    "RTE Business": (_gn("rte.ie/news/business"), "IE", "Europe", "en"),                             # [22]
    "Kathimerini": (_gn("ekathimerini.com", macro=True), "GR", "Europe", "en"),                      # [15]
    "Die Presse": (_gn("diepresse.com", "de-AT", macro=True), "AT", "Europe", "de"),                 # [46]
    "ECO": (_gn("eco.sapo.pt", "pt-PT", macro=True), "PT", "Europe", "pt"),                          # [71]
    # Central and Eastern Europe
    "BBJ": (_gn("bbj.hu"), "HU", "CEE", "en"),                                                      # [10]
    "Daily News Hungary": (_gn("dailynewshungary.com"), "HU", "CEE", "en"),                         # [4]
    "Telex English": (_gn("telex.hu/english"), "HU", "CEE", "en"),                                  # [5]
    "Hungary Today": ("https://hungarytoday.hu/feed/", "HU", "CEE", "en"),                          # [5]
    "TVP World": (_gn("tvpworld.com", macro=True), "PL", "CEE", "en"),                              # [13]
    "Notes from Poland": ("https://notesfrompoland.com/feed/", "PL", "CEE", "en"),                  # [9]
    "PAP English": (_gn("pap.pl/en"), "PL", "CEE", "en"),                                           # [2]
    "Radio Prague Intl": (_gn("english.radio.cz"), "CZ", "CEE", "en"),                              # [28]
    "bne IntelliNews": (_gn("intellinews.com"), "CEE", "CEE", "en"),                                # [40]
    # Israel, South Africa
    "Globes": (_gn("en.globes.co.il"), "IL", "CEEMEA", "en"),                                       # [8]
    "Times of Israel": (_gn("timesofisrael.com", macro=True), "IL", "CEEMEA", "en"),                # [38]
    "Moneyweb": (_gn("moneyweb.co.za"), "ZA", "CEEMEA", "en"),                                      # [57]
    "News24 Business": (_gn("news24.com/business"), "ZA", "CEEMEA", "en"),                          # [28]
    "Daily Maverick": (_gn("dailymaverick.co.za", macro=True), "ZA", "CEEMEA", "en"),               # [5]
    # Latin America
    "Valor International": (_gn("valorinternational.globo.com"), "BR", "LatAm", "en"),              # [3]
    "Folha English": (_gn("folha.uol.com.br/internacional/en"), "BR", "LatAm", "en"),               # [2]
    "Valor Economico": (_gn("valor.globo.com", "pt-BR", macro=True), "BR", "LatAm", "pt"),          # [74]
    "Mexico News Daily": ("https://mexiconewsdaily.com/feed/", "MX", "LatAm", "en"),                # [10]
    "El Financiero": (_gn("elfinanciero.com.mx", "es-MX", macro=True), "MX", "LatAm", "es"),        # [40]
    "La Republica": (_gn("larepublica.co", "es-CO", macro=True), "CO", "LatAm", "es"),              # [47]
    "Valora Analitik": (_gn("valoraanalitik.com", "es-CO", macro=True), "CO", "LatAm", "es"),       # [20]
    "Diario Financiero": (_gn("df.cl", "es-CL", macro=True), "CL", "LatAm", "es"),                  # [40]
    "La Tercera Pulso": (_gn("latercera.com/pulso", "es-CL"), "CL", "LatAm", "es"),                 # [37]
    "MercoPress": (_gn("en.mercopress.com"), "LatAm", "LatAm", "en"),                               # [11]
    "Bloomberg Linea": (_gn("bloomberglinea.com", "es-MX", macro=True), "LatAm", "LatAm", "es"),    # [28]
    # Asia ex-Japan
    "Yonhap": (_gn("en.yna.co.kr", macro=True), "KR", "APAC", "en"),                                # [13]
    "Korea Times": (_gn("koreatimes.co.kr", macro=True), "KR", "APAC", "en"),                       # [26]
    "Business Times SG": (_gn("businesstimes.com.sg"), "SG", "APAC", "en"),                         # [13]
    "Straits Times": (_gn("straitstimes.com", macro=True), "SG", "APAC", "en"),                     # [32]
    "The Edge Malaysia": (_gn("theedgemalaysia.com"), "MY", "APAC", "en"),                          # [8]
    "The Star Business": (_gn("thestar.com.my/business"), "MY", "APAC", "en"),                      # [3]
    "Bangkok Post Business": ("https://www.bangkokpost.com/rss/data/business.xml", "TH", "APAC", "en"),  # [10]
    "Nation Thailand": (_gn("nationthailand.com", macro=True), "TH", "APAC", "en"),                 # [13]
    "Nikkei Asia": (_gn("asia.nikkei.com"), "APAC", "APAC", "en"),                                  # [36]
    # Japan, Australia, New Zealand
    "Japan Times Business": (_gn("japantimes.co.jp/business"), "JP", "APAC-DM", "en"),             # [14]
    "AFR": (_gn("afr.com"), "AU", "APAC-DM", "en"),                                                 # [40]
    "interest.co.nz": (_gn("interest.co.nz"), "NZ", "APAC-DM", "en"),                               # [16]
    "NZ Herald Business": (_gn("nzherald.co.nz/business"), "NZ", "APAC-DM", "en"),                  # [7]
}
local_csv_path = f"data/localpress_{today}.csv"
_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"

# === Scraping Function ===
def scrape_article_content(url):
    try:
        article = Article(url)
        article.download()
        article.parse()
        return article.text
    except Exception as e:
        print(f"Error scraping {url}: {e}")
        return ""

# === Main Scraper ===
def main():
    os.makedirs("data", exist_ok=True)

    # Only avoid duplicates within today's file (day-to-day); do not check history
    seen_links = set()
    seen_titles = set()
    if os.path.exists(csv_path):
        try:
            df = pd.read_csv(csv_path)
            seen_links.update(df["link"].tolist())
            seen_titles.update(df["title"].tolist())
        except Exception as e:
            print(f"Warning: Could not read {csv_path}: {e}")
    print(f"Loaded {len(seen_links)} existing links and {len(seen_titles)} existing titles (today only)")

    new_articles = []

    for source, url in rss_feeds.items():
        feed = feedparser.parse(url)
        for entry in feed.entries:
            # Check both link AND title to prevent duplicates
            if entry.link not in seen_links and entry.title not in seen_titles:
                content = scrape_article_content(entry.link) if scrape_content else ""
                new_articles.append({
                    "timestamp": datetime.now().isoformat(timespec='seconds'),
                    "source": source,
                    "title": entry.title,
                    "link": entry.link,
                    "published": entry.get("published", "N/A"),
                    "content": content
                })
                seen_links.add(entry.link)
                seen_titles.add(entry.title)

    if new_articles:
        df_new = pd.DataFrame(new_articles)
        
        # Load today's existing file if it exists
        if os.path.exists(csv_path):
            df_existing = pd.read_csv(csv_path)
            df_combined = pd.concat([df_existing, df_new], ignore_index=True)
        else:
            df_combined = df_new
            
        df_combined.to_csv(csv_path, index=False)
        print(f"✅ {len(new_articles)} new articles saved to {csv_path}")
    else:
        print("No new articles found.")


# === Local press ===
def _published(entry):
    for k in ("published", "updated"):
        v = entry.get(k)
        if v:
            try:
                d = parsedate_to_datetime(v)
                return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
            except Exception:
                pass
    return None


def local_due(now=None):
    """True when today's local-press file is missing or its newest row is older than LOCAL_EVERY_H hours."""
    if not os.path.exists(local_csv_path):
        return True
    try:
        last = pd.to_datetime(pd.read_csv(local_csv_path, usecols=["timestamp"])["timestamp"]).max()
    except Exception:
        return True
    return (now or datetime.now()) - last >= timedelta(hours=LOCAL_EVERY_H) - timedelta(minutes=5)


def scrape_local(save=True):
    """Fetch the local-press feeds into data/localpress_<date>.csv. A feed that fails is skipped, never fatal.
    Dedupes against today's and yesterday's local file (search feeds re-serve the same items for days)."""
    os.makedirs("data", exist_ok=True)
    seen_links, seen_titles = set(), set()
    yest = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    for p in (local_csv_path, f"data/localpress_{yest}.csv"):
        if os.path.exists(p):
            try:
                d = pd.read_csv(p, usecols=["title", "link"])
                seen_links.update(d["link"].tolist())
                seen_titles.update(d["title"].tolist())
            except Exception as e:
                print(f"Warning: could not read {p}: {e}")
    cutoff = datetime.now(timezone.utc) - timedelta(days=LOCAL_MAX_AGE_D)
    rows, per_feed = [], {}
    for source, (url, market, region, lang) in local_feeds.items():
        try:
            feed = feedparser.parse(url, agent=_UA)
        except Exception as e:
            print(f"  {source}: failed ({e})")
            continue
        n = 0
        for entry in feed.entries:
            title, link = entry.get("title"), entry.get("link")
            if not title or not link or link in seen_links or title in seen_titles:
                continue
            pub = _published(entry)
            if pub is not None and pub < cutoff:
                continue
            rows.append({"timestamp": datetime.now().isoformat(timespec="seconds"), "source": source, "market": market,
                         "region": region, "lang": lang, "title": title, "link": link,
                         "published": entry.get("published", "N/A")})
            seen_links.add(link)
            seen_titles.add(title)
            n += 1
        per_feed[source] = n
    if save and rows:
        df = pd.DataFrame(rows)
        if os.path.exists(local_csv_path):
            df = pd.concat([pd.read_csv(local_csv_path), df], ignore_index=True)
        df.to_csv(local_csv_path, index=False)
        print(f"✅ {len(rows)} new local-press items saved to {local_csv_path}")
    elif not rows:
        print("No new local-press items.")
    return rows, per_feed


# === Entry Point ===
if __name__ == "__main__":
    main()
    if local_due():
        scrape_local()
    else:
        print(f"Local press fetched less than {LOCAL_EVERY_H}h ago; skipped this run.")
