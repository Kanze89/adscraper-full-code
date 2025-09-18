# -*- coding: utf-8 -*-
"""
run.py — Orchestrator for ikon.mn + gogo.mn + news.mn (scrapers stay unchanged)

What it does:
- Runs all three scrapers (or selected ones via flags)
- Uses your folder: C:\Users\tuguldur.kh\Downloads\adscraper-full-code
- Builds an Excel with clickable links from the combined CSV
- On Monday only: zips last 7 days of screenshots and emails ZIP + ledger + Excel
- Optionally git commit & push (if env configured)

No changes to gogo_mn.py, ikon_mn.py, news_mn.py required.
"""

import os, shutil, argparse
from datetime import datetime
from pathlib import Path

from shipping import build_xlsx_from_csv, zip_last_7_days, send_email, git_commit_and_push

def import_optional(module_name: str):
    try:
        return __import__(module_name, fromlist=["*"])
    except Exception as e:
        print(f"[WARN] Could not import {module_name}: {e}")
        return None

# ========= Paths (as requested) =========
BASE_DIR_WIN = r"C:\Users\tuguldur.kh\Downloads\adscraper-full-code"
DEFAULT_OUTPUT_ROOT = os.path.abspath(os.path.join(BASE_DIR_WIN, "banner_screenshots"))
DEFAULT_CSV_PATH    = os.path.abspath(os.path.join(BASE_DIR_WIN, "banner_tracking_combined.csv"))
DEFAULT_LEDGER_PATH = os.path.abspath(os.path.join(BASE_DIR_WIN, "banner_master.csv"))
DEFAULT_XLSX_PATH   = os.path.abspath(os.path.join(BASE_DIR_WIN, "banner_tracking_combined.xlsx"))
DEFAULT_ZIP_PATH    = os.path.abspath(os.path.join(BASE_DIR_WIN, "weekly_banners.zip"))
# =======================================

def backup_csv(csv_path: str) -> None:
    if os.path.exists(csv_path):
        try:
            bak_path = csv_path + ".bak"
            shutil.copy2(csv_path, bak_path)
            print(f"[INFO] Backed up old CSV -> {bak_path}")
        except Exception as e:
            print(f"[!] Could not back up old CSV: {e}. Continuing.")

def warm_tldextract():
    try:
        import tldextract
        tldextract.extract("example.com")
        print("[INFO] tldextract cache ready")
    except Exception:
        print("[INFO] tldextract not installed; using heuristic eTLD+1.")

def _set_common_env(ledger_path: str, max_mins: int, req_timeout_ms: int):
    os.environ["BANNER_LEDGER"] = ledger_path
    os.environ["SCRAPE_MAX_MINUTES"] = str(max_mins)
    os.environ["REQUEST_TIMEOUT_MS"] = str(req_timeout_ms)

def run_ikon(output_root: str, csv_path: str, skip_gifs: bool) -> None:
    mod = import_optional("ikon_mn")
    if not mod or not hasattr(mod, "scrape_ikon_ad_banners"):
        print("[SKIP] ikon.mn scraper not available.")
        return
    try:
        mod.scrape_ikon_ad_banners(
            output_root=output_root,
            csv_path=csv_path,
            skip_gifs=skip_gifs
        )
    except Exception as e:
        print(f"[ERROR] ikon.mn scraping failed: {e}")

def run_gogo(output_root: str, csv_path: str, skip_gifs: bool) -> None:
    mod = import_optional("gogo_mn")
    if not mod or not hasattr(mod, "scrape_gogo_banners"):
        print("[SKIP] gogo.mn scraper not available.")
        return
    try:
        mod.scrape_gogo_banners(
            output_root=output_root,
            csv_path=csv_path,
            skip_gifs=skip_gifs
        )
    except Exception as e:
        print(f"[ERROR] gogo.mn scraping failed: {e}")

def run_news(output_root: str, csv_path: str, skip_gifs: bool) -> None:
    mod = import_optional("news_mn")
    if not mod or not hasattr(mod, "scrape_news_banners"):
        print("[SKIP] news.mn scraper not available.")
        return
    try:
        mod.scrape_news_banners(
            output_root=output_root,
            csv_path=csv_path,
            skip_gifs=skip_gifs
        )
    except Exception as e:
        print(f"[ERROR] news.mn scraping failed: {e}")

def _maybe_email_weekly(output_root: str, combined_csv: str, xlsx_path: str, zip_path: str, ledger_path: str):
    # Always rebuild Excel from combined CSV (adds clickable links)
    build_xlsx_from_csv(combined_csv, xlsx_path)

    # Only Monday => zip + send
    if datetime.now().weekday() == 0:  # Monday
        zip_last_7_days(output_root, zip_path)
        subject = f"[Adscraper] Weekly banners — {datetime.now():%Y-%m-%d}"
        body = "Attached: last 7 days banner screenshots (zip), banner ledger, and Excel with clickable links."
        attachments = [zip_path]
        if os.path.exists(ledger_path):
            attachments.append(ledger_path)
        if os.path.exists(xlsx_path):
            attachments.append(xlsx_path)
        send_email(subject, body, attachments)
    else:
        print("[INFO] Not Monday — skipping weekly email.")

def _git_push_repo():
    repo_dir = os.getenv("GIT_REPO_DIR", BASE_DIR_WIN)
    msg = f"adscraper update {datetime.now():%Y-%m-%d %H:%M}"
    git_commit_and_push(repo_dir, msg)

def main():
    parser = argparse.ArgumentParser(description="Run ikon.mn, gogo.mn, news.mn + ship (no scraper code changes).")
    parser.add_argument("--output", dest="output_root", default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--csv", dest="csv_path", default=DEFAULT_CSV_PATH)
    parser.add_argument("--ledger", dest="ledger_path", default=DEFAULT_LEDGER_PATH)
    parser.add_argument("--xlsx", dest="xlsx_path", default=DEFAULT_XLSX_PATH)
    parser.add_argument("--zip", dest="zip_path", default=DEFAULT_ZIP_PATH)
    parser.add_argument("--no-skip-gifs", action="store_true")
    parser.add_argument("--ikon", action="store_true")
    parser.add_argument("--gogo", action="store_true")
    parser.add_argument("--news", action="store_true")
    parser.add_argument("--max-mins", type=int, default=6)
    parser.add_argument("--req-timeout-ms", type=int, default=10000)
    args = parser.parse_args()

    output_root = os.path.abspath(args.output_root)
    csv_path    = os.path.abspath(args.csv_path)
    ledger_path = os.path.abspath(args.ledger_path)
    xlsx_path   = os.path.abspath(args.xlsx_path)
    zip_path    = os.path.abspath(args.zip_path)
    skip_gifs   = not args.no_skip_gifs

    Path(output_root).mkdir(parents=True, exist_ok=True)
    backup_csv(csv_path)
    warm_tldextract()
    _set_common_env(ledger_path, args.max_mins, args.req_timeout_ms)

    run_all = not (args.ikon or args.gogo or args.news)
    if run_all:
        print("[START] Scanning ikon.mn, gogo.mn, news.mn")
        run_ikon(output_root, csv_path, skip_gifs)
        run_gogo(output_root, csv_path, skip_gifs)
        run_news(output_root, csv_path, skip_gifs)
        print("[DONE] All sites scanned.")
    else:
        print("[START] Custom selection")
        if args.ikon:
            print("  -> ikon.mn")
            run_ikon(output_root, csv_path, skip_gifs)
        if args.gogo:
            print("  -> gogo.mn")
            run_gogo(output_root, csv_path, skip_gifs)
        if args.news:
            print("  -> news.mn")
            run_news(output_root, csv_path, skip_gifs)
        print("[DONE] Selected sites scanned.")

    # Ship stuff
    _maybe_email_weekly(output_root, csv_path, xlsx_path, zip_path, ledger_path)
    _git_push_repo()

if __name__ == "__main__":
    main()
