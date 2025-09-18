# -*- coding: utf-8 -*-
"""
ikon_mn.py — robust time-delayed capture for ikon.mn /ad/ pages.
"""

import os, csv, time, re, hashlib
from urllib.parse import urljoin, urlparse
from datetime import datetime
from typing import List, Dict, Optional, Set, Tuple

from playwright.sync_api import sync_playwright, Page, BrowserContext, TimeoutError as PWTimeout
from banner_ledger import BannerLedger

IKON_HOME      = "https://ikon.mn"
AD_PATH_HINT   = "/ad/"

# ---- TIMING ----
HOMEPAGE_IDLE_SECONDS = 6
RELOAD_ROUNDS         = 4
ROUND_SECONDS         = 20
POLL_SECONDS          = 2

# ---- FILTERS ----
MIN_W, MIN_H = 300, 100
SKIP_GIFS    = True

# ---- TIMEOUTS / HARD STOP ----
SCRAPE_MAX_MINUTES = int(os.getenv("SCRAPE_MAX_MINUTES", "6"))
REQUEST_TIMEOUT_MS = int(os.getenv("REQUEST_TIMEOUT_MS", "10000"))

def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def today_str() -> str:
    return datetime.now().strftime("%Y-%m-%d")

def md5_short_bytes(b: bytes) -> str:
    return hashlib.md5(b).hexdigest()[:10]

def join_url(base: str, maybe_rel: str) -> str:
    try:
        return urljoin(base, maybe_rel)
    except Exception:
        return maybe_rel

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

def _host(u: str) -> str:
    try:
        return urlparse(u).netloc.lower()
    except Exception:
        return ""

def _etld1_naive(host: str) -> str:
    if not host:
        return ""
    parts = host.split(".")
    return ".".join(parts[-2:]) if len(parts) >= 2 else host

def _collect_ad_links_dom(page: Page) -> Set[str]:
    ad_links: Set[str] = set()

    iframe_loc = page.locator("iframe[src*='/ad/'], iframe[src^='/ad/'], iframe[src*='//ikon.mn/ad/']")
    for i in range(iframe_loc.count()):
        try:
            src = iframe_loc.nth(i).get_attribute("src") or ""
            if AD_PATH_HINT in src:
                ad_links.add(join_url(IKON_HOME, src))
        except Exception:
            pass

    a_loc = page.locator("a[href*='/ad/'], a[href^='/ad/'], a[href*='//ikon.mn/ad/']")
    for i in range(a_loc.count()):
        try:
            href = a_loc.nth(i).get_attribute("href") or ""
            if AD_PATH_HINT in href:
                ad_links.add(join_url(IKON_HOME, href))
        except Exception:
            pass

    return ad_links

def _collect_ad_links_network(context: BrowserContext, page: Page, settle_seconds: int) -> Set[str]:
    ad_links: Set[str] = set()

    def maybe_add(url: str):
        if AD_PATH_HINT in url and "ikon.mn" in urlparse(url).netloc:
            ad_links.add(url)

    context.on("request", lambda req: maybe_add(req.url))
    page.on("request",     lambda req: maybe_add(req.url))

    t_end = time.time() + settle_seconds
    while time.time() < t_end:
        try:
            page.mouse.wheel(0, 1200)
            page.wait_for_timeout(250)
        except Exception:
            break

    page.wait_for_timeout(400)
    return ad_links

def _collect_ad_links_from_frames(page: Page) -> Set[str]:
    out: Set[str] = set()
    try:
        for fr in page.frames:
            try:
                url = fr.url
                if AD_PATH_HINT in url and "ikon.mn" in urlparse(url).netloc:
                    out.add(url)
            except Exception:
                continue
    except Exception:
        pass
    return out

def find_ad_links_on_home(context: BrowserContext, page: Page) -> List[str]:
    collected: Set[str] = set()
    collected |= _collect_ad_links_dom(page)
    collected |= _collect_ad_links_network(context, page, HOMEPAGE_IDLE_SECONDS)
    collected |= _collect_ad_links_from_frames(page)

    normalized = set()
    for u in collected:
        try:
            if not u:
                continue
            absu = join_url(IKON_HOME, u)
            if "ikon.mn" in urlparse(absu).netloc and AD_PATH_HINT in absu:
                normalized.add(absu)
        except Exception:
            continue
    return sorted(normalized)

_AD_HINT_ATTRS = ["hostname", "host", "data-host", "data-advertiser", "data-company", "data-brand", "data-domain"]

def _advertiser_hint_from_block(block) -> str:
    try:
        a = block.locator("a[href]").first
        if a.count() > 0:
            href_abs = a.evaluate("(el) => el.href || ''") or ""
            if href_abs:
                return _host(href_abs)
    except Exception:
        pass
    try:
        ifr = block.locator("iframe").first
        if ifr.count() > 0:
            for attr in _AD_HINT_ATTRS:
                v = ifr.get_attribute(attr)
                if v:
                    return v
    except Exception:
        pass
    return ""

def watch_and_save_all_variants(context: BrowserContext, ad_url: str, out_dir: str,
                                ledger: BannerLedger, today: str, site_deadline: float) -> List[Dict]:
    rows: List[Dict] = []
    seen_hashes: Set[str] = set()

    page = context.new_page()
    try:
        page.set_default_timeout(15000)
        page.set_default_navigation_timeout(15000)

        for round_idx in range(RELOAD_ROUNDS):
            if time.time() >= site_deadline:
                print("[TIMEOUT] ikon.mn hard stop reached", flush=True)
                return rows

            page.goto(ad_url, wait_until="domcontentloaded")
            page.wait_for_timeout(500)

            t_end = time.time() + ROUND_SECONDS
            while time.time() < t_end:
                if time.time() >= site_deadline:
                    print("[TIMEOUT] ikon.mn hard stop reached", flush=True)
                    return rows

                containers = page.locator("[data-controller='banner'] div.banner")
                if containers.count() == 0:
                    imgs = page.locator("img[data-banner-target='item']")
                    rows.extend(_scan_img_locators(context, page, ad_url, imgs, out_dir, seen_hashes, ledger, today, "", site_deadline))
                else:
                    for i in range(containers.count()):
                        block = containers.nth(i)
                        adv_hint = _advertiser_hint_from_block(block)
                        img_loc = block.locator("img[data-banner-target='item']")
                        if img_loc.count() == 0:
                            continue
                        rows.extend(_scan_img_locators(context, page, ad_url, img_loc, out_dir, seen_hashes, ledger, today, adv_hint, site_deadline))
                page.wait_for_timeout(POLL_SECONDS * 1000)

        if not any(r for r in rows if not r["skipped_reason"]):
            rows.append(_row(ad_url, "", "", "", 0, 0, "no_new_creatives"))

        return rows

    except PWTimeout:
        rows.append(_row(ad_url, "", "", "", 0, 0, "timeout"))
        return rows
    except Exception as e:
        rows.append(_row(ad_url, "", "", "", 0, 0, f"error:{e.__class__.__name__}"))
        return rows
    finally:
        try:
            page.close()
        except Exception:
            pass

def _scan_img_locators(context: BrowserContext, page: Page, ad_url: str, img_loc, out_dir: str,
                       seen_hashes: Set[str], ledger: BannerLedger, today: str, adv_hint: str,
                       site_deadline: float) -> List[Dict]:
    rows: List[Dict] = []
    count = img_loc.count()
    for j in range(count):
        if time.time() >= site_deadline:
            print("[TIMEOUT] ikon.mn hard stop reached", flush=True)
            break

        el = img_loc.nth(j)
        try:
            src = el.get_attribute("src") or ""
            data_src = el.get_attribute("data-src") or ""
            img_url = src or data_src
            if not img_url:
                rows.append(_row(ad_url, "", "", "", 0, 0, "empty_url"))
                continue

            abs_url = join_url(ad_url, img_url)
            if SKIP_GIFS and is_gif(abs_url):
                rows.append(_row(ad_url, abs_url, "", "", 0, 0, "gif_skipped"))
                continue

            w, h = parse_dims_from_filename(abs_url)
            if w is None or h is None:
                try:
                    box = el.bounding_box()
                    if box:
                        w, h = int(box.get("width") or 0), int(box.get("height") or 0)
                except Exception:
                    pass
            if (w and h) and (w < MIN_W or h < MIN_H):
                rows.append(_row(ad_url, abs_url, "", "", w or 0, h or 0, "too_small"))
                continue

            try:
                resp = context.request.get(abs_url, timeout=REQUEST_TIMEOUT_MS)
            except Exception:
                rows.append(_row(ad_url, abs_url, "", "", w or 0, h or 0, "download_timeout"))
                continue

            if not resp.ok:
                rows.append(_row(ad_url, abs_url, "", "", w or 0, h or 0, f"download_failed_{resp.status}"))
                continue
            content = resp.body()
            hsh = md5_short_bytes(content)

            if hsh in seen_hashes:
                rows.append(_row(ad_url, abs_url, "", "", w or 0, h or 0, "duplicate_hash"))
                continue

            ext = os.path.splitext(urlparse(abs_url).path)[1] or ".bin"
            fname = f"ikon_banner_{int(time.time())}_{hsh}{ext}"
            fpath = os.path.join(out_dir, fname)
            print(f"[ikon] saving {abs_url}", flush=True)
            save_bytes(fpath, content)
            seen_hashes.add(hsh)

            click_url = _guess_click_url(page)

            banner_id, match_type = ledger.observe_image(
                img_bytes=content,
                site="ikon.mn",
                example_path=fpath,
                seen_date=today,
                click_url=click_url,
                asset_url=abs_url,
                page_url=ad_url,
                advertiser_hint=adv_hint
            )
            print(f"[LEDGER] {banner_id} {match_type} first={ledger.rows[banner_id]['first_seen_date']} "
                  f"last={ledger.rows[banner_id]['last_seen_date']} days_seen={ledger.rows[banner_id]['days_seen']} "
                  f"adv={ledger.rows[banner_id]['advertiser_host']}", flush=True)

            print(f"[NEW] ikon.mn banner (/ad) -> {fpath}", flush=True)
            rows.append(_row(ad_url, abs_url, fpath, click_url, w or 0, h or 0, ""))

        except Exception as e:
            rows.append(_row(ad_url, "", "", "", 0, 0, f"error:{e.__class__.__name__}"))
    return rows

def _guess_click_url(page: Page) -> str:
    try:
        anchors = page.locator("[data-controller='banner'] div.banner a[href]")
        n = anchors.count()
        if n == 0:
            return ""
        page_dom = _etld1_naive(_host(page.url))
        first_fallback = ""
        for i in range(n):
            a = anchors.nth(i)
            href_abs = a.evaluate("(el) => el.href || ''") or ""
            if not href_abs:
                continue
            if not first_fallback:
                first_fallback = href_abs
            h = _host(href_abs)
            if _etld1_naive(h) and _etld1_naive(h) != page_dom:
                return href_abs
        return first_fallback
    except Exception:
        return ""

def _row(ad_url: str, image_url: str, image_path: str, click_url: str, w: int, h: int, reason: str) -> Dict:
    return {
        "site": "ikon.mn",
        "date": today_str(),
        "ad_url": ad_url,
        "image_url": image_url,
        "click_url": click_url,
        "image_path": image_path,
        "width": w,
        "height": h,
        "skipped_reason": reason
    }

def scrape_ikon_ad_banners(output_root: str,
                           csv_path: str,
                           skip_gifs: bool = True) -> None:
    date_dir = os.path.join(output_root, "ikon.mn", today_str())
    ensure_dir(date_dir)

    ledger_csv = os.getenv("BANNER_LEDGER", "./banner_master.csv")
    ledger = BannerLedger(csv_path=os.path.abspath(ledger_csv))
    today = today_str()
    site_deadline = time.time() + SCRAPE_MAX_MINUTES * 60

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()
        page.set_default_timeout(15000)
        page.set_default_navigation_timeout(15000)

        page.goto(IKON_HOME, wait_until="domcontentloaded")
        page.wait_for_timeout(800)

        ad_links = find_ad_links_on_home(context, page)
        if ad_links:
            print(f"[INFO] Found {len(ad_links)} /ad/ link(s)", flush=True)
        else:
            print("[INFO] No /ad/ links found on ikon.mn", flush=True)

        rows: List[Dict] = []
        for ad_url in ad_links:
            if time.time() >= site_deadline:
                print("[TIMEOUT] ikon.mn hard stop reached before visiting all /ad/ links", flush=True)
                break
            rows.extend(watch_and_save_all_variants(
                context=context,
                ad_url=ad_url,
                out_dir=date_dir,
                ledger=ledger,
                today=today,
                site_deadline=site_deadline
            ))

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
    CSV_PATH    = os.path.abspath(os.path.join(BASE_DIR_WIN, "banner_tracking_ikon.csv"))
    os.makedirs(OUTPUT_ROOT, exist_ok=True)
    scrape_ikon_ad_banners(
        output_root=OUTPUT_ROOT,
        csv_path=CSV_PATH,
        skip_gifs=True
    )
