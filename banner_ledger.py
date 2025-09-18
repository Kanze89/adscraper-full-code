# -*- coding: utf-8 -*-
"""
banner_ledger.py — persistent ledger to track unique ad banners across days,
with advertiser attribution and linkable example URLs for Excel/email/sharing.

New fields:
- example_rel     (path relative to OUTPUT_ROOT)
- example_url     (clickable URL if PUBLIC_BASE_URL is set)
Retains: example_path for backward-compat but you should rely on URL/REL.

Environment:
- BANNER_LEDGER_PHASH_DIST  (default 6)
- PUBLIC_BASE_URL           e.g. https://github.com/you/repo/blob/main/
"""

import csv, os, hashlib, io
from datetime import datetime
from typing import Dict, Tuple, Optional
from urllib.parse import urlparse

from PIL import Image
import imagehash

try:
    import tldextract
except Exception:
    tldextract = None

LEDGER_FIELDS = [
    "banner_id", "site", "first_seen_date", "last_seen_date",
    "days_seen", "seen_dates",
    # file refs
    "example_path", "example_rel", "example_url",
    # fingerprints
    "md5", "phash", "matches",
    # attribution
    "advertiser_host", "advertiser_domain", "source_host", "iframe_host", "page_domain",
    "advertiser_hosts_all", "advertiser_domains_all",
]

AD_HOST_SKIP = {
    "boost.mn", "edge.boost.mn", "exchange.boost.mn",
    "doubleclick.net", "googlesyndication.com", "adservice.google.com",
}

MAX_HASH_DISTANCE = int(os.getenv("BANNER_LEDGER_PHASH_DIST", "6"))

def _today() -> str:
    return datetime.now().strftime("%Y-%m-%d")

def _md5_short(b: bytes) -> str:
    return hashlib.md5(b).hexdigest()[:10]

def _phash_hex(b: bytes) -> str:
    im = Image.open(io.BytesIO(b)).convert("RGB")
    return str(imagehash.phash(im))

def _host_from_url(url: Optional[str]) -> str:
    if not url:
        return ""
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return ""

def _host_from_hint(hint: Optional[str]) -> str:
    if not hint:
        return ""
    s = hint.strip().lower()
    if not s:
        return ""
    if "://" in s or s.startswith("//"):
        try:
            h = urlparse(s if "://" in s else ("http:" + s)).netloc
            return (h or "").lower()
        except Exception:
            pass
    return s

def _etld1_from_host(host: str) -> str:
    if not host:
        return ""
    if tldextract:
        ext = tldextract.extract(host)
        if ext.domain and ext.suffix:
            return f"{ext.domain}.{ext.suffix}"
    parts = host.split(".")
    return ".".join(parts[-2:]) if len(parts) >= 2 else host

def _add_unique(row: dict, key: str, value: str) -> None:
    if not value:
        return
    cur = row.get(key) or ""
    parts = [p for p in cur.split(";") if p]
    if value not in parts:
        parts.append(value)
    row[key] = ";".join(parts)

def _public_url_from_rel(example_rel: str) -> str:
    """
    Build a shareable URL if PUBLIC_BASE_URL is configured.
    Example: PUBLIC_BASE_URL=https://github.com/USER/REPO/blob/main/
    """
    base = (os.getenv("PUBLIC_BASE_URL") or "").strip()
    if not base or not example_rel:
        return ""
    if not base.endswith("/"):
        base += "/"
    return base + example_rel.replace("\\", "/")

class BannerLedger:
    def __init__(self, csv_path: str):
        self.csv_path = csv_path
        self.rows: Dict[str, Dict] = {}
        self._by_md5: Dict[str, str] = {}
        self._by_phash: Dict[str, str] = {}
        self._load()

    def _load(self):
        if not os.path.exists(self.csv_path):
            return
        with open(self.csv_path, "r", newline="", encoding="utf-8") as f:
            r = csv.DictReader(f)
            for row in r:
                bid = row.get("banner_id")
                if not bid:
                    continue
                # backfill missing new fields
                for k in LEDGER_FIELDS:
                    row.setdefault(k, "")
                self.rows[bid] = row
                if row.get("md5"):
                    self._by_md5[row["md5"]] = bid
                if row.get("phash"):
                    self._by_phash[row["phash"]] = bid

    def _alloc_banner_id(self, md5_short: str) -> str:
        return f"bn_{md5_short}"

    def _phash_dist(self, a_hex: str, b_hex: str) -> int:
        return imagehash.hex_to_hash(a_hex) - imagehash.hex_to_hash(b_hex)

    def _find_by_phash_near(self, phash_hex: str) -> Optional[str]:
        if phash_hex in self._by_phash:
            return self._by_phash[phash_hex]
        best_id, best_dist = None, 10**9
        for bid, row in self.rows.items():
            rph = row.get("phash") or ""
            if not rph:
                continue
            d = self._phash_dist(phash_hex, rph)
            if d < best_dist:
                best_dist, best_id = d, bid
        if best_id and best_dist <= MAX_HASH_DISTANCE:
            return best_id
        return None

    def _choose_advertiser(self, advertiser_hint: Optional[str],
                           click_url: Optional[str],
                           page_url: Optional[str]) -> Tuple[str, str]:
        page_dom = _etld1_from_host(_host_from_url(page_url)) if page_url else ""

        hint_host = _host_from_hint(advertiser_hint)
        if hint_host:
            etld1 = _etld1_from_host(hint_host)
            if etld1 and etld1 not in AD_HOST_SKIP and etld1 != page_dom:
                return hint_host, etld1

        host = _host_from_url(click_url)
        if host:
            etld1 = _etld1_from_host(host)
            if etld1 and etld1 not in AD_HOST_SKIP and etld1 != page_dom:
                return host, etld1

        return "", ""

    def observe_image(
        self,
        img_bytes: bytes,
        site: str,
        *,
        example_path: str,
        example_rel: Optional[str] = None,  # NEW
        seen_date: Optional[str] = None,
        click_url: Optional[str] = None,
        asset_url: Optional[str] = None,
        page_url: Optional[str] = None,
        iframe_src: Optional[str] = None,
        advertiser_hint: Optional[str] = None
    ) -> Tuple[str, str]:
        """
        Returns (banner_id, match_type) where match_type ∈ {"exact","near","new"}.
        """
        seen_date = seen_date or _today()
        md5s = _md5_short(img_bytes)
        ph   = _phash_hex(img_bytes)

        if md5s in self._by_md5:
            bid, mtype = self._by_md5[md5s], "exact"
        else:
            near_id = self._find_by_phash_near(ph)
            if near_id:
                bid, mtype = near_id, "near"
            else:
                bid, mtype = self._alloc_banner_id(md5s), "new"
                self.rows[bid] = {
                    "banner_id": bid, "site": site,
                    "first_seen_date": seen_date, "last_seen_date": seen_date,
                    "days_seen": "1", "seen_dates": seen_date,
                    "example_path": example_path, "example_rel": example_rel or "", "example_url": "",
                    "md5": md5s, "phash": ph, "matches": mtype,
                    "advertiser_host": "", "advertiser_domain": "",
                    "source_host": "", "iframe_host": "", "page_domain": "",
                    "advertiser_hosts_all": "", "advertiser_domains_all": "",
                }

        row = self.rows[bid]
        row["site"] = row.get("site") or site

        # dates
        row["first_seen_date"] = min(row.get("first_seen_date","") or seen_date, seen_date)
        row["last_seen_date"]  = max(row.get("last_seen_date","")  or seen_date, seen_date)
        prev = set(filter(None, (row.get("seen_dates") or "").split(";")))
        prev.add(seen_date)
        row["seen_dates"] = ";".join(sorted(prev))
        row["days_seen"]  = str(len(prev))

        # keep the earliest concrete example_path but update rel/url each run
        if not row.get("example_path"):
            row["example_path"] = example_path
        if example_rel:
            row["example_rel"] = example_rel
            row["example_url"] = _public_url_from_rel(example_rel)

        # attribution
        adv_host, adv_domain = self._choose_advertiser(advertiser_hint, click_url, page_url)
        if adv_host:   row["advertiser_host"]   = adv_host
        if adv_domain: row["advertiser_domain"] = adv_domain
        _add_unique(row, "advertiser_hosts_all", adv_host)
        _add_unique(row, "advertiser_domains_all", adv_domain)

        src_host = _host_from_url(asset_url)
        if src_host and not row.get("source_host"):
            row["source_host"] = src_host
        if iframe_src:
            ifh = _host_from_url(iframe_src)
            if ifh and not row.get("iframe_host"):
                row["iframe_host"] = ifh
        pg_host = _host_from_url(page_url)
        pg_dom  = _etld1_from_host(pg_host) if pg_host else ""
        if pg_dom and not row.get("page_domain"):
            row["page_domain"] = pg_dom

        # refresh prints + index
        row["md5"] = md5s
        row["phash"] = ph
        row["matches"] = mtype
        self._by_md5[md5s] = bid
        self._by_phash[ph] = bid
        return bid, mtype

    def save(self):
        d = os.path.dirname(self.csv_path)
        if d:
            os.makedirs(d, exist_ok=True)
        with open(self.csv_path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=LEDGER_FIELDS)
            w.writeheader()
            for row in self.rows.values():
                for k in LEDGER_FIELDS:
                    row.setdefault(k, "")
                w.writerow(row)
