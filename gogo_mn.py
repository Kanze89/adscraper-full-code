# -*- coding: utf-8 -*-
"""
gogo_mn.py — Reliable advertiser capture for gogo.mn (edge.boost.mn).
"""

import os, csv, time, re, hashlib, json, base64
from urllib.parse import urljoin, urlparse, parse_qs, urlsplit
from datetime import datetime
from typing import List, Dict, Optional, Set, Tuple

from playwright.sync_api import (
    sync_playwright, Page, BrowserContext, Route, Request
)

SITE_NAME       = "gogo.mn"
GOGO_HOME       = "https://gogo.mn/"
BOOST_HOST_HINT = "edge.boost.mn"

# ======= Tuning =======
RELOAD_ROUNDS      = 3
ROUND_SECONDS      = 20
POLL_SECONDS       = 1
HOMEPAGE_IDLE_MS   = 500
SCROLL_PASSES      = 4
SCROLL_PAUSE_MS    = 240
SCROLL_STEP_PX     = 1200
WAIT_FOR_SLIDES_MS = 12000   # wait for AD slides

MIN_W, MIN_H = 280, 100
SKIP_GIFS    = True

# Debug (safe)
DEBUG_DETECT = False
MAX_DEBUG_MAP_PRINT = 8

# Treat as infra/social, not advertisers
AD_HOST_SKIP = {
    "boost.mn", "edge.boost.mn", "exchange.boost.mn",
    "doubleclick.net", "googlesyndication.com", "adservice.google.com",
    "gogo.mn", "www.gogo.mn",
}
SOCIAL_SKIP = {
    "facebook.com", "www.facebook.com", "m.facebook.com", "fb.com",
    "x.com", "twitter.com", "t.co",
    "instagram.com", "www.instagram.com",
    "youtube.com", "www.youtube.com", "youtu.be",
    "linkedin.com", "www.linkedin.com",
    "t.me", "telegram.me",
    "pinterest.com", "www.pinterest.com",
}

LIKELY_SLOTS = [
    "div.swiper[data-banner-type='AD'] div.swiper-slide",
]

BLOCKED_SUBSTRINGS = [
    "googletagmanager.com", "google-analytics.com", "doubleclick.net",
    "facebook.net", "facebook.com", "hotjar.com", "segment.com",
    "fonts.googleapis.com", "fonts.gstatic.com",
    ".woff", ".woff2", ".ttf", ".otf",
    ".mp4", ".webm", ".m3u8", ".mp3"
]

# ======= Helpers =======
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

def parse_dims_from_filename(url: str):
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

try:
    import tldextract
except Exception:
    tldextract = None

def _etld1(host: str) -> str:
    if not host:
        return ""
    if tldextract:
        ext = tldextract.extract(host)
        if ext.domain and ext.suffix:
            return f"{ext.domain}.{ext.suffix}"
    parts = host.split(".")
    return ".".join(parts[-2:]) if len(parts) >= 2 else host

def _choose_advertiser_host(click_url: str, page_url: str) -> str:
    """Return advertiser host (keeps www) or '' if same-domain/infra/social."""
    h = _host(click_url)
    if not h:
        return ""
    if _etld1(h) == _etld1(_host(page_url)):
        return ""
    if h in AD_HOST_SKIP or _etld1(h) in AD_HOST_SKIP:
        return ""
    if h in SOCIAL_SKIP or _etld1(h) in SOCIAL_SKIP:
        return ""
    return h  # keep www

def _row(page_url: str, slot_id: int, image_url: str, click_url: str, advertiser_host: str,
         image_path: str, w: int, h: int, reason: str) -> Dict:
    return {
        "site": SITE_NAME,
        "date": today_str(),
        "page_url": page_url,
        "slot_id": slot_id,
        "image_url": image_url,
        "click_url": click_url,
        "advertiser_host": advertiser_host,
        "image_path": image_path,
        "width": w,
        "height": h,
        "skipped_reason": reason
    }

# ======= Request blocking (keep Boost) =======
def should_block(req: Request) -> bool:
    url = req.url.lower()
    if BOOST_HOST_HINT in url:
        return False
    host = urlparse(url).netloc
    if "gogo.mn" in host:
        if any(s in url for s in [".woff", ".woff2", ".ttf", ".otf", ".mp4", ".webm", ".m3u8", ".mp3"]):
            return True
        return False
    if any(s in url for s in BLOCKED_SUBSTRINGS):
        return True
    return False

def install_blocking(context: BrowserContext) -> None:
    def handler(route: Route, request: Request):
        try:
            if should_block(request):
                return route.abort()
            return route.continue_()
        except Exception:
            return route.continue_()
    context.route("**/*", handler)

# ======= Scroll =======
def full_page_scroll(page: Page, passes: int, step_px: int, pause_ms: int) -> None:
    for _ in range(passes):
        page.evaluate(
            """async (step) => {
                const maxH = () => Math.max(
                    document.body.scrollHeight, document.documentElement.scrollHeight,
                    document.body.offsetHeight, document.documentElement.offsetHeight,
                    document.body.clientHeight, document.documentElement.clientHeight
                );
                while (window.scrollY + window.innerHeight + 10 < maxH()) {
                    window.scrollBy(0, step);
                    await new Promise(r => setTimeout(r, 18));
                }
            }""",
            step_px
        )
        page.wait_for_timeout(pause_ms)
        page.evaluate("window.scrollTo(0, 0)")
        page.wait_for_timeout(100)

def _likely_slot_locators(page: Page) -> List:
    loc = page.locator(", ".join(LIKELY_SLOTS))
    cnt = loc.count()
    return [loc.nth(i) for i in range(cnt)] if cnt else [page.locator("body")]

# ======= Click resolvers =======
def _is_useless_href(h: str) -> bool:
    h = (h or "").strip()
    return (not h) or h == "#" or h == "/"

def _normalize_click_choice(page_url: str, dest_attr: str, a_href: str) -> str:
    """
    Prefer slide's data-click-destination when anchor is empty or same-domain.
    Otherwise use anchor href.
    """
    dest_attr = (dest_attr or "").strip()
    a_href = (a_href or "").strip()

    if dest_attr and not _is_useless_href(dest_attr):
        # If anchor is same-domain, prefer the slide attr
        try:
            ap = urlparse(a_href) if a_href else None
            pp = urlparse(page_url)
            if (not a_href) or (not ap.netloc) or (ap.netloc == pp.netloc):
                return join_url(page_url, dest_attr)
        except Exception:
            pass
        # If anchor points off-site and dest_attr exists too, prefer off-site
        if a_href:
            try:
                ap = urlparse(a_href)
                pp = urlparse(page_url)
                if ap.netloc and ap.netloc != pp.netloc:
                    return join_url(page_url, a_href)
            except Exception:
                pass
        return join_url(page_url, dest_attr)

    if a_href and not _is_useless_href(a_href):
        return join_url(page_url, a_href)

    return ""

def _decode_click_from_event_url(event_url: str) -> str:
    """
    Best-effort parse of Boost `data-click-event` URL.
    """
    if not event_url:
        return ""
    try:
        q = parse_qs(urlsplit(event_url).query)
        b64 = (q.get("data") or [""])[0]
        if not b64:
            return ""
        pad = "=" * ((4 - len(b64) % 4) % 4)
        raw = base64.urlsafe_b64decode((b64 + pad).encode("utf-8"))
        payload = json.loads(raw.decode("utf-8", errors="ignore"))
        candidates = []
        for key in ("href","url","dest","destination","click","link","hostname","host"):
            v = payload.get(key)
            if isinstance(v, str) and v.strip():
                candidates.append(v.strip())
        for v in candidates:
            try:
                u = urlparse(v if "://" in v else ("https://" + v))
                if u.netloc:
                    return u.geturl()
            except Exception:
                continue
        return ""
    except Exception:
        return ""

# ======= Slide map (evaluate) =======
def _harvest_slide_map(page: Page, page_url: str) -> Dict[str, Tuple[str, str]]:
    try:
        result = page.evaluate("""(pageUrl, BOOST) => {
            const toAbs = (u) => { try { return new URL(u, pageUrl).href; } catch { return u || ''; } };
            const isUselessHref = (h) => !h || h === '#' || h === '/' || h.trim() === '';
            const out = {};
            const root = document.querySelector("div.swiper[data-banner-type='AD']") || document;
            const slides = Array.from(root.querySelectorAll('div.swiper-slide'));
            for (const sl of slides) {
                const a = sl.querySelector('a[href]');
                const aHref = a && a.href ? a.href : '';
                const destAttr = sl.getAttribute('data-click-destination') || '';
                let chosen = destAttr;
                if (isUselessHref(chosen) && aHref) chosen = aHref;
                chosen = chosen ? toAbs(chosen) : '';

                const add = (u) => {
                    if (!u) return;
                    if (u.indexOf(BOOST) === -1) return;
                    const abs = toAbs(u);
                    if (!out[abs]) out[abs] = {
                        chosen,
                        aHrefAbs: aHref ? toAbs(aHref) : '',
                        destAttrAbs: destAttr ? toAbs(destAttr) : '',
                        clickEvent: sl.getAttribute('data-click-event') || ''
                    };
                };

                sl.querySelectorAll('img').forEach(img => {
                    ['src','data-src','data-original','data-lazy','data-url'].forEach(attr => {
                        const v = img.getAttribute(attr) || ''; if (v) add(v);
                    });
                    const ss = img.getAttribute('srcset') || '';
                    if (ss) ss.split(',').forEach(part => {
                        const t = part.trim().split(' ')[0].trim(); if (t) add(t);
                    });
                });
                sl.querySelectorAll('picture source[srcset]').forEach(s => {
                    const ss = s.getAttribute('srcset') || '';
                    if (ss) ss.split(',').forEach(part => {
                        const t = part.trim().split(' ')[0].trim(); if (t) add(t);
                    });
                });
                sl.querySelectorAll('video[poster]').forEach(v => {
                    const p = v.getAttribute('poster') || ''; if (p) add(p);
                });
                ['data-media-src','data-dynamic-media-src'].forEach(attr => {
                    const v = sl.getAttribute(attr) || ''; if (v) add(v);
                });
            }
            return out;
        }""", page_url, BOOST_HOST_HINT)
    except Exception:
        result = {}

    out: Dict[str, Tuple[str, str]] = {}
    for asset_abs, meta in result.items():
        a_abs = (meta.get("aHrefAbs") or "").strip()
        d_abs = (meta.get("destAttrAbs") or "").strip()
        chosen = _normalize_click_choice(page_url, d_abs, a_abs)
        if not chosen:
            ev = (meta.get("clickEvent") or "").strip()
            decoded = _decode_click_from_event_url(ev)
            if decoded:
                chosen = decoded
        adv = _choose_advertiser_host(chosen, page_url) if chosen else ""
        out[asset_abs] = (chosen, adv)

    if DEBUG_DETECT:
        print(f"[DETECT] slide_map size={len(out)}", flush=True)
        c = 0
        for k, (cu, adv) in out.items():
            base = os.path.basename(urlparse(k).path)
            print(f"[DETECT]  slide_map {base} -> click={cu or '-'} adv={adv or '-'}", flush=True)
            c += 1
            if c >= MAX_DEBUG_MAP_PRINT: break
    return out

# ======= Token helpers & fallbacks =======
def _tokens_from_boost_path(asset_url: str) -> List[str]:
    try:
        path = urlparse(asset_url).path
    except Exception:
        path = asset_url
    segs = [s for s in path.split("/") if s]
    out = []
    if segs: out.append(segs[-1])
    if len(segs) >= 2 and len(segs[-2]) >= 8: out.append(segs[-2])
    return out

def _resolve_click_by_elements(page: Page, asset_url: str, page_url: str) -> Tuple[str, str]:
    tokens = _tokens_from_boost_path(asset_url)
    if not tokens:
        return "", ""
    base_parts = [
        'img[src*="{t}"]','img[data-src*="{t}"]','img[srcset*="{t}"]',
        'source[srcset*="{t}"]','video[poster*="{t}"]','*[style*="{t}"]'
    ]
    for t in tokens:
        selector = ", ".join(p.format(t=t) for p in base_parts)
        loc = page.locator(selector)
        for i in range(min(8, loc.count())):
            h = loc.nth(i).element_handle()
            if not h: continue
            try:
                dest_attr = h.evaluate("(el)=>{ const sl=el.closest('.swiper-slide'); return sl? (sl.getAttribute('data-click-destination')||'') : ''; }") or ""
            except Exception:
                dest_attr = ""
            try:
                a_href = h.evaluate("(el)=>{ const a=el.closest('.swiper-slide')?.querySelector('a[href]'); return a && a.href ? a.href : ''; }") or ""
            except Exception:
                a_href = ""
            chosen = _normalize_click_choice(page_url, dest_attr, a_href)
            if not chosen:
                try:
                    ev = h.evaluate("(el)=>{ const sl=el.closest('.swiper-slide'); return sl? (sl.getAttribute('data-click-event')||'') : ''; }") or ""
                except Exception:
                    ev = ""
                decoded = _decode_click_from_event_url(ev)
                chosen = decoded or ""
            adv = _choose_advertiser_host(chosen, page_url) if chosen else ""
            if chosen:
                return chosen, adv
    return "", ""

def _resolve_click_by_slide_html(page: Page, asset_url: str, page_url: str) -> Tuple[str, str]:
    tokens = _tokens_from_boost_path(asset_url)
    if not tokens:
        return "", ""
    try:
        chosen = page.evaluate("""(pageUrl, tokens) => {
            const toAbs = (u) => { try { return new URL(u, pageUrl).href; } catch { return u || ''; } };
            const isUselessHref = (h) => !h || h === '#' || h === '/' || h.trim() === '';
            const root = document.querySelector("div.swiper[data-banner-type='AD']") || document;
            const slides = Array.from(root.querySelectorAll('div.swiper-slide'));
            for (const sl of slides) {
                const html = sl.outerHTML || '';
                let hit = false;
                for (const t of tokens) { if (t && html.indexOf(t) !== -1) { hit = true; break; } }
                if (!hit) continue;

                const a = sl.querySelector('a[href]');
                const aHref = a && a.href ? a.href : '';
                const destAttr = sl.getAttribute('data-click-destination') || '';
                let chosen = destAttr;
                if (isUselessHref(chosen) && aHref) chosen = aHref;
                return chosen ? toAbs(chosen) : '';
            }
            return '';
        }""", page_url, tokens) or ""
    except Exception:
        chosen = ""
    if not chosen:
        try:
            ev = page.evaluate("""(tokens) => {
                const root = document.querySelector("div.swiper[data-banner-type='AD']") || document;
                const slides = Array.from(root.querySelectorAll('div.swiper-slide'));
                for (const sl of slides) {
                    const html = sl.outerHTML || '';
                    let hit = false;
                    for (const t of tokens) { if (t && html.indexOf(t) !== -1) { hit = true; break; } }
                    if (!hit) continue;
                    return sl.getAttribute('data-click-event') || '';
                }
                return '';
            }""", tokens) or ""
        except Exception:
            ev = ""
        decoded = _decode_click_from_event_url(ev)
        chosen = decoded or ""
    adv = _choose_advertiser_host(chosen, page_url) if chosen else ""
    return chosen, adv

def _slot_click_for_locator(slot_locator, page_url: str) -> Tuple[str, str]:
    def normalize(dest: str, a_href: str) -> str:
        return _normalize_click_choice(page_url, dest or "", a_href or "")

    click = ""
    try:
        a_href = slot_locator.evaluate("(sl)=>{ const a=sl.querySelector('a[href]'); return a && a.href ? a.href : ''; }") or ""
    except Exception:
        a_href = ""
    try:
        dest_attr = slot_locator.evaluate("(sl)=> sl.getAttribute('data-click-destination') || ''") or ""
    except Exception:
        dest_attr = ""
    click = normalize(dest_attr, a_href)
    if not click:
        try:
            ev = slot_locator.evaluate("(sl)=> sl.getAttribute('data-click-event') || ''") or ""
        except Exception:
            ev = ""
        click = _decode_click_from_event_url(ev) or ""
    adv = _choose_advertiser_host(click, page_url) if click else ""
    if DEBUG_DETECT:
        print(f"[DETECT] slot_click -> {click or '-'} adv={adv or '-'}", flush=True)
    return click or "", adv or ""

# ======= CSV + Ledger =======
try:
    from banner_ledger import BannerLedger
except Exception as e:
    BannerLedger = None
    print(f"[WARN] banner_ledger.py not found ({e}). Ledger updates will be skipped.", flush=True)

def _env_ledger_path(default_root: str) -> str:
    # Use env if provided, else keep ledger inside output_root\ledger\
    path = os.getenv("BANNER_LEDGER", "").strip()
    if path:
        return os.path.abspath(path)
    return os.path.abspath(os.path.join(default_root, "ledger", "banner_ledger.csv"))

def scrape_gogo_banners(output_root: str,
                        csv_path: str,
                        skip_gifs: bool = True,
                        ledger_csv_path: Optional[str] = None) -> None:
    date_dir = os.path.join(output_root, SITE_NAME, today_str())
    ensure_dir(date_dir)

    ledger = None
    if BannerLedger is not None:
        ledger_csv_path = ledger_csv_path or _env_ledger_path(output_root)
        ensure_dir(os.path.dirname(ledger_csv_path))
        ledger = BannerLedger(ledger_csv_path)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(java_script_enabled=True)
        install_blocking(context)

        boost_from_network: Set[str] = set()
        def on_req(req: Request):
            try:
                if BOOST_HOST_HINT in req.url:
                    boost_from_network.add(req.url)
            except Exception:
                pass
        context.on("request", on_req)

        page = context.new_page()
        page.set_default_timeout(18000)

        rows: List[Dict] = []
        content_hashes_seen: Set[str] = set()

        for round_idx in range(RELOAD_ROUNDS):
            if round_idx == 0:
                page.goto(GOGO_HOME, wait_until="domcontentloaded")
            else:
                page.reload(wait_until="domcontentloaded")
            page.wait_for_timeout(HOMEPAGE_IDLE_MS)

            try:
                page.wait_for_selector("div.swiper[data-banner-type='AD'] div.swiper-slide",
                                       timeout=WAIT_FOR_SLIDES_MS, state="attached")
            except Exception:
                pass

            full_page_scroll(page, SCROLL_PASSES, SCROLL_STEP_PX, SCROLL_PAUSE_MS)

            deadline = time.time() + ROUND_SECONDS
            per_slot_seen_urls: Dict[int, Set[str]] = {}

            while time.time() < deadline:
                slide_map = _harvest_slide_map(page, page.url)
                cur_slots = _likely_slot_locators(page)
                nslots = len(cur_slots)
                if nslots == 0:
                    break

                for slot_id in range(nslots):
                    slot = cur_slots[slot_id]
                    slot_click, slot_adv = _slot_click_for_locator(slot, page.url)
                    seen = per_slot_seen_urls.setdefault(slot_id, set())
                    scope = slot.locator("img, picture source[srcset], video[poster]")
                    for i in range(scope.count()):
                        el = scope.nth(i)
                        assets: Set[str] = set()
                        try:
                            tag = el.evaluate("(n)=>n.tagName.toLowerCase()")
                            if tag == "img":
                                for attr in ("src","data-src","data-original","data-lazy","data-url"):
                                    v = el.get_attribute(attr) or ""
                                    if v and BOOST_HOST_HINT in v: assets.add(join_url(page.url, v))
                                ss = el.get_attribute("srcset") or ""
                                if ss:
                                    for part in ss.split(","):
                                        t = (part.strip().split(" ")[0]).strip()
                                        if t and BOOST_HOST_HINT in t: assets.add(join_url(page.url, t))
                            elif tag == "source":
                                ss = el.get_attribute("srcset") or ""
                                if ss:
                                    for part in ss.split(","):
                                        t = (part.strip().split(" ")[0]).strip()
                                        if t and BOOST_HOST_HINT in t: assets.add(join_url(page.url, t))
                            else:
                                pstr = el.get_attribute("poster") or ""
                                if pstr and BOOST_HOST_HINT in pstr: assets.add(join_url(page.url, pstr))
                        except Exception:
                            continue

                        for asset_abs in assets:
                            if asset_abs in seen:
                                continue
                            seen.add(asset_abs)

                            click, adv = "", ""
                            if asset_abs in slide_map:
                                click, adv = slide_map[asset_abs]
                            if not click:
                                click, adv = _resolve_click_by_elements(page, asset_abs, page.url)
                            if not click:
                                click, adv = _resolve_click_by_slide_html(page, asset_abs, page.url)
                            if not click and slot_click:
                                click, adv = slot_click, slot_adv

                            if SKIP_GIFS and is_gif(asset_abs):
                                rows.append(_row(page.url, slot_id, asset_abs, click, adv, "", 0, 0, "gif_skipped"))
                                continue

                            w, h = parse_dims_from_filename(asset_abs)
                            resp = context.request.get(asset_abs)
                            if not resp.ok:
                                rows.append(_row(page.url, slot_id, asset_abs, click, adv, "", w or 0, h or 0, f"download_failed_{resp.status}"))
                                continue

                            content = resp.body()
                            file_hash = md5_short_bytes(content)
                            if file_hash in content_hashes_seen:
                                rows.append(_row(page.url, slot_id, asset_abs, click, adv, "", w or 0, h or 0, "duplicate_hash"))
                                continue

                            ext = os.path.splitext(urlparse(asset_abs).path)[1] or ".bin"
                            fname = f"gogo_boost_{int(time.time())}_{file_hash}{ext}"
                            fpath = os.path.join(date_dir, fname)
                            save_bytes(fpath, content)
                            content_hashes_seen.add(file_hash)

                            if ledger:
                                try:
                                    bid, mtype = ledger.observe_image(
                                        content,
                                        site=SITE_NAME,
                                        example_path=fpath,
                                        seen_date=today_str(),
                                        click_url=click or "",
                                        asset_url=asset_abs or "",
                                        page_url=page.url or "",
                                        iframe_src="",
                                        advertiser_hint=(adv or "")
                                    )
                                    row = ledger.rows.get(bid, {})
                                    adv_print = row.get("advertiser_host","")
                                    first = row.get("first_seen_date","")
                                    last  = row.get("last_seen_date","")
                                    days  = row.get("days_seen","")
                                    print(f"[LEDGER] {bid} {mtype} first={first} last={last} days_seen={days} adv={adv_print}", flush=True)
                                except Exception as e:
                                    print(f"[WARN] ledger.observe_image failed: {e}", flush=True)

                            print(f"[NEW] {SITE_NAME} banner (slot {slot_id}) -> {fpath} adv={adv or '-'}", flush=True)
                            rows.append(_row(page.url, slot_id, asset_abs, click, adv, fpath, w or 0, h or 0, ""))

                # network-seen assets (slot 0)
                if boost_from_network:
                    seen0 = per_slot_seen_urls.setdefault(0, set())
                    for u in sorted(boost_from_network):
                        abs_u = join_url(page.url, u)
                        if abs_u in seen0:
                            continue
                        seen0.add(abs_u)

                        click, adv = "", ""
                        if abs_u in slide_map:
                            click, adv = slide_map[abs_u]
                        if not click:
                            click, adv = _resolve_click_by_elements(page, abs_u, page.url)
                        if not click:
                            click, adv = _resolve_click_by_slide_html(page, abs_u, page.url)
                        if not click:
                            slots = _likely_slot_locators(page)
                            for sl in slots:
                                click, adv = _slot_click_for_locator(sl, page.url)
                                if click:
                                    break

                        if SKIP_GIFS and is_gif(abs_u):
                            rows.append(_row(page.url, 0, abs_u, click, adv, "", 0, 0, "gif_skipped"))
                            continue

                        w, h = parse_dims_from_filename(abs_u)
                        resp = context.request.get(abs_u)
                        if not resp.ok:
                            rows.append(_row(page.url, 0, abs_u, click, adv, "", w or 0, h or 0, f"download_failed_{resp.status}"))
                            continue

                        content = resp.body()
                        file_hash = md5_short_bytes(content)
                        if file_hash in content_hashes_seen:
                            rows.append(_row(page.url, 0, abs_u, click, adv, "", w or 0, h or 0, "duplicate_hash"))
                            continue

                        ext = os.path.splitext(urlparse(abs_u).path)[1] or ".bin"
                        fname = f"gogo_boost_{int(time.time())}_{file_hash}{ext}"
                        fpath = os.path.join(date_dir, fname)
                        save_bytes(fpath, content)
                        content_hashes_seen.add(file_hash)

                        if ledger:
                            try:
                                bid, mtype = ledger.observe_image(
                                    content,
                                    site=SITE_NAME,
                                    example_path=fpath,
                                    seen_date=today_str(),
                                    click_url=click or "",
                                    asset_url=abs_u or "",
                                    page_url=page.url or "",
                                    iframe_src="",
                                    advertiser_hint=(adv or "")
                                )
                                row = ledger.rows.get(bid, {})
                                adv_print = row.get("advertiser_host","")
                                first = row.get("first_seen_date","")
                                last  = row.get("last_seen_date","")
                                days  = row.get("days_seen","")
                                print(f"[LEDGER] {bid} {mtype} first={first} last={last} days_seen={days} adv={adv_print}", flush=True)
                            except Exception as e:
                                print(f"[WARN] ledger.observe_image failed: {e}", flush=True)

                        print(f"[NEW] {SITE_NAME} banner (slot 0) -> {fpath} adv={adv or '-'}", flush=True)
                        rows.append(_row(page.url, 0, abs_u, click, adv, fpath, w or 0, h or 0, ""))

                page.mouse.wheel(0, SCROLL_STEP_PX)
                page.wait_for_timeout(POLL_SECONDS * 1000)

        if ledger:
            try:
                ledger.save()
            except Exception as e:
                print(f"[WARN] ledger.save() failed: {e}", flush=True)

        if rows:
            ensure_dir(os.path.dirname(csv_path))
            write_header = not os.path.exists(csv_path)
            with open(csv_path, "a", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
                if write_header:
                    w.writeheader()
                for r in rows:
                    w.writerow(r)

        context.close()
        browser.close()


# CLI defaults for standalone runs (pointed to your folder)
if __name__ == "__main__":
    BASE_DIR_WIN = r"C:\Users\tuguldur.kh\Downloads\adscraper-full-code"
    OUTPUT_ROOT = os.path.abspath(os.path.join(BASE_DIR_WIN, "banner_screenshots"))
    CSV_PATH    = os.path.abspath(os.path.join(BASE_DIR_WIN, "banner_tracking_gogo.csv"))
    os.makedirs(OUTPUT_ROOT, exist_ok=True)
    scrape_gogo_banners(
        output_root=OUTPUT_ROOT,
        csv_path=CSV_PATH,
        skip_gifs=True,
        ledger_csv_path=None  # will respect BANNER_LEDGER if set
    )
