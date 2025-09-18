# -*- coding: utf-8 -*-
"""
news_mn.py — STRICT banner capture for news.mn
"""

import os, csv, time, re, hashlib
from urllib.parse import urljoin, urlparse
from datetime import datetime
from typing import List, Dict, Optional, Set, Tuple

from playwright.sync_api import (
    sync_playwright, Page, BrowserContext,
    TimeoutError as PWTimeout
)

from banner_ledger import BannerLedger

NEWS_HOME    = "https://news.mn/"
UPLOADS_HINT = "news.mn/wp-content/uploads"

RELOAD_ROUNDS    = 2
ROUND_SECONDS    = 10
POLL_SECONDS     = 1
SCROLL_PASSES    = 3
SCROLL_STEP_PX   = 1400
SCROLL_PAUSE_MS  = 200
HOMEPAGE_IDLE_MS = 500

MIN_W, MIN_H = 280, 100
SKIP_GIFS    = True

SCRAPE_MAX_MINUTES = int(os.getenv("SCRAPE_MAX_MINUTES", "6"))
REQUEST_TIMEOUT_MS = int(os.getenv("REQUEST_TIMEOUT_MS", "10000"))

def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def today_str() -> str:
    return datetime.now().strftime("%Y-%m-%d")

def md5_short_bytes(b: bytes) -> str:
    return hashlib.md5(b).hexdigest()[:10]

def join_url(base: str, maybe_rel: str) -> str:
    return urljoin(base, maybe_rel)

def is_gif(url: str) -> bool:
    return urlparse(url).path.lower().endswith(".gif")

def save_bytes(path: str, data: bytes) -> None:
    ensure_dir(os.path.dirname(path))
    with open(path, "wb") as f:
        f.write(data)

def parse_dims_from_filename(url: str) -> Tuple[Optional[int], Optional[int]]:
    m = re.search(r'(\d{2,5})x(\d{2,5})(?=[^\d]|$)', url)
    if m:
        try:
            return int(m.group(1)), int(m.group(2))
        except Exception:
            pass
    return None, None

def _row(page_url: str, slot_id: int, image_url: str, click_url: str,
         image_path: str, w: int, h: int, reason: str) -> Dict:
    return {
        "site": "news.mn",
        "date": today_str(),
        "page_url": page_url,
        "slot_id": slot_id,
        "image_url": image_url,
        "click_url": click_url,
        "image_path": image_path,
        "width": w,
        "height": h,
        "skipped_reason": reason
    }

def full_page_scroll(page: Page, passes: int, step_px: int, pause_ms: int) -> None:
    for _ in range(passes):
        try:
            page.evaluate(
                """async (step) => {
                    let maxH = () => Math.max(
                        document.body.scrollHeight, document.documentElement.scrollHeight,
                        document.body.offsetHeight, document.documentElement.offsetHeight,
                        document.body.clientHeight, document.documentElement.clientHeight
                    );
                    while (window.scrollY + window.innerHeight + 10 < maxH()) {
                        window.scrollBy(0, step);
                        await new Promise(r => setTimeout(r, 16));
                    }
                }""",
                step_px
            )
        except Exception:
            pass
        page.wait_for_timeout(pause_ms)
        page.evaluate("window.scrollTo(0, 0)")
        page.wait_for_timeout(120)

BANNER_ITEM_SEL = ".news-banner-container .it-banner-slider-item"
VIDEO_POSTER_SEL = "video[poster]"
IMG_FALLBACK_SEL = "img"
_AD_HINT_ATTRS = ["hostname", "host", "data-host", "data-advertiser", "data-company", "data-brand", "data-domain"]

def _host(u: str) -> str:
    try:
        return urlparse(u).netloc.lower()
    except Exception:
        return ""

def _advertiser_hint(scope) -> str:
    try:
        a = scope.locator("a[href]").first
        if a.count() > 0:
            href_abs = a.evaluate("(el) => el.href || ''") or ""
            if href_abs:
                return _host(href_abs)
    except Exception:
        pass
    try:
        ifr = scope.locator("iframe").first
        if ifr.count() > 0:
            for attr in _AD_HINT_ATTRS:
                v = ifr.get_attribute(attr)
                if v:
                    return v
    except Exception:
        pass
    return ""

def _iframe_src(scope) -> str:
    try:
        ifr = scope.locator("iframe[src]").first
        if ifr.count() > 0:
            return ifr.get_attribute("src") or ""
    except Exception:
        pass
    return ""

def _collect_banner_items(page: Page) -> List[Dict]:
    items: List[Dict] = []
    containers = page.locator(BANNER_ITEM_SEL)
    count = containers.count()
    for i in range(count):
        item = containers.nth(i)

        click_url = ""
        try:
            a = item.locator("a[href]").first
            if a.count() > 0:
                href_abs = a.evaluate("(el) => el.href || ''") or ""
                if href_abs:
                    click_url = href_abs
        except Exception:
            pass

        poster_url = ""
        try:
            v = item.locator(VIDEO_POSTER_SEL).first
            if v.count() > 0:
                poster = v.get_attribute("poster") or ""
                if poster and UPLOADS_HINT in poster:
                    poster_url = join_url(page.url, poster)
        except Exception:
            pass

        img_fallback = ""
        if not poster_url:
            try:
                img = item.locator(IMG_FALLBACK_SEL).first
                if img.count() > 0:
                    for attr in ("src", "data-src"):
                        val = img.get_attribute(attr) or ""
                        if val and UPLOADS_HINT in val:
                            img_fallback = join_url(page.url, val)
                            break
            except Exception:
                pass

        asset_url = poster_url or img_fallback
        if asset_url:
            items.append({
                "slot_id": i,
                "click_url": click_url,
                "asset_url": asset_url,
                "adv_hint": _advertiser_hint(item),
                "iframe_src": _iframe_src(item)
            })
    return items

def scrape_news_banners(output_root: str,
                        csv_path: str,
                        skip_gifs: bool = True) -> None:
    date_dir = os.path.join(output_root, "news.mn", today_str())
    ensure_dir(date_dir)

    ledger_csv = os.getenv("BANNER_LEDGER", "./banner_master.csv")
    ledger = BannerLedger(csv_path=os.path.abspath(ledger_csv))
    today = today_str()
    hard_deadline = time.time() + SCRAPE_MAX_MINUTES * 60

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(java_script_enabled=True)
        page = context.new_page()
        page.set_default_timeout(15000)
        page.set_default_navigation_timeout(15000)

        rows: List[Dict] = []
        file_hashes_seen: Set[str] = set()
        url_seen_this_run: Set[str] = set()

        for round_idx in range(RELOAD_ROUNDS):
            if time.time() >= hard_deadline:
                print("[TIMEOUT] news.mn hard stop reached", flush=True)
                break

            if round_idx == 0:
                page.goto(NEWS_HOME, wait_until="domcontentloaded")
            else:
                page.reload(wait_until="domcontentloaded")
            page.wait_for_timeout(HOMEPAGE_IDLE_MS)

            full_page_scroll(page, SCROLL_PASSES, SCROLL_STEP_PX, SCROLL_PAUSE_MS)

            deadline = time.time() + ROUND_SECONDS
            while time.time() < deadline:
                if time.time() >= hard_deadline:
                    print("[TIMEOUT] news.mn hard stop reached", flush=True)
                    break

                banners = _collect_banner_items(page)
                for b in banners:
                    slot_id   = b["slot_id"]
                    asset_url = b["asset_url"]
                    click_url = b["click_url"]
                    adv_hint  = b["adv_hint"]
                    iframe_src = b["iframe_src"]

                    if not asset_url or UPLOADS_HINT not in asset_url:
                        continue
                    if SKIP_GIFS and is_gif(asset_url):
                        rows.append(_row(page.url, slot_id, asset_url, click_url, "", 0, 0, "gif_skipped"))
                        continue
                    if asset_url in url_seen_this_run:
                        rows.append(_row(page.url, slot_id, asset_url, click_url, "", 0, 0, "same_url_skipped"))
                        continue

                    w, h = parse_dims_from_filename(asset_url)
                    if (w and h) and (w < MIN_W or h < MIN_H):
                        rows.append(_row(page.url, slot_id, asset_url, click_url, "", w or 0, h or 0, "too_small"))
                        continue

                    try:
                        resp = context.request.get(asset_url, timeout=REQUEST_TIMEOUT_MS)
                    except Exception:
                        rows.append(_row(page.url, slot_id, asset_url, click_url, "", w or 0, h or 0, "download_timeout"))
                        continue

                    if not resp.ok:
                        rows.append(_row(page.url, slot_id, asset_url, click_url, "", w or 0, h or 0, f"download_failed_{resp.status}"))
                        continue

                    content = resp.body()
                    file_hash = md5_short_bytes(content)
                    if file_hash in file_hashes_seen:
                        rows.append(_row(page.url, slot_id, asset_url, click_url, "", w or 0, h or 0, "duplicate_hash"))
                        url_seen_this_run.add(asset_url)
                        continue

                    ext = os.path.splitext(urlparse(asset_url).path)[1] or ".bin"
                    fname = f"news_banner_{int(time.time())}_{file_hash}{ext}"
                    fpath = os.path.join(date_dir, fname)
                    print(f"[news] saving {asset_url}", flush=True)
                    save_bytes(fpath, content)

                    file_hashes_seen.add(file_hash)
                    url_seen_this_run.add(asset_url)

                    banner_id, match_type = ledger.observe_image(
                        img_bytes=content,
                        site="news.mn",
                        example_path=fpath,
                        seen_date=today,
                        click_url=click_url,
                        asset_url=asset_url,
                        page_url=page.url,
                        iframe_src=iframe_src,
                        advertiser_hint=adv_hint
                    )
                    print(f"[LEDGER] {banner_id} {match_type} first={ledger.rows[banner_id]['first_seen_date']} "
                          f"last={ledger.rows[banner_id]['last_seen_date']} days_seen={ledger.rows[banner_id]['days_seen']} "
                          f"adv={ledger.rows[banner_id]['advertiser_host']}", flush=True)

                    print(f"[NEW] news.mn banner (slot {slot_id}) -> {fpath}", flush=True)
                    rows.append(_row(page.url, slot_id, asset_url, click_url, fpath, w or 0, h or 0, ""))

                page.mouse.wheel(0, SCROLL_STEP_PX)
                page.wait_for_timeout(POLL_SECONDS * 1000)

        if rows:
            ensure_dir(os.path.dirname(csv_path))
            write_header = not os.path.exists(csv_path)
            with open(csv_path, "a", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
                if write_header:
                    w.writeheader()
                for r in rows:
                    w.writerow(r)

        ledger.save()
        context.close()
        browser.close()


if __name__ == "__main__":
    BASE_DIR_WIN = r"C:\Users\tuguldur.kh\Downloads\adscraper-full-code"
    OUTPUT_ROOT = os.path.abspath(os.path.join(BASE_DIR_WIN, "banner_screenshots"))
    CSV_PATH    = os.path.abspath(os.path.join(BASE_DIR_WIN, "banner_tracking_news.csv"))
    os.makedirs(OUTPUT_ROOT, exist_ok=True)
    scrape_news_banners(
        output_root=OUTPUT_ROOT,
        csv_path=CSV_PATH,
        skip_gifs=True
    )
