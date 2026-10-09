# EM_catalysts

A light test copy of `dr-macro/catalyst` (main @ 5ecaf879, 2026-10-09), made to try the local-press feeds without
touching the original.

What differs from the original:
- `rss_scraper.py`: adds 60 local-press feeds (Europe 22, CEE 9, Israel/South Africa 5, Latin America 11, Asia ex-Japan
  9, Japan/Australia/New Zealand 4), written to `data/localpress_<date>.csv`, fetched at most every 3 hours. The global
  feeds and `data/articles_<date>.csv` are unchanged.
- `.github/workflows/`: only `rss.yml` is kept (the daily pipeline and calendar scraper are not run here); `rss.yml`
  asks for `contents: write` so its commit step can push.
- No `data/`, `kg/` or `summaries/` history (they stay in the original).

When the feeds have proven themselves, the `rss_scraper.py` change goes to the original repo, and the collected
`data/localpress_*.csv` files can be copied into the original's `data/` folder.
