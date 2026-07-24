#!/usr/bin/env python3
"""
Verify a list of candidate RSS/Atom feed URLs for digital-marketing / social-media
news sources, and record which ones actually work.

For every source we try a list of candidate URLs (in order) and keep the first
one that passes all checks:
  1. HTTP status 200
  2. Content is valid XML (RSS / Atom / RDF) - not an HTML page
  3. The feed contains at least one item/entry
  4. We can determine the publish date of the most recent item

A feed that parses fine but whose newest item is older than DEAD_AFTER_DAYS
days is flagged as "dead" (stale) rather than "active".

Output:
  feeds_verified.json - machine readable list of the sources that are ACTIVE
  feeds_report.md     - human readable report in Hebrew
"""

import json
import re
import sys
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import requests

DEAD_AFTER_DAYS = 60
REQUEST_TIMEOUT = (10, 20)  # (connect, read) seconds
MAX_WORKERS = 8

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/rss+xml, application/atom+xml, application/xml;q=0.9, text/xml;q=0.8, */*;q=0.5",
    "Accept-Language": "en-US,en;q=0.8,he;q=0.6",
}

# name -> (is_extra, [candidate urls in order of preference])
SOURCES = {
    "Meta Newsroom": (False, [
        "https://about.fb.com/news/feed/",
        "https://about.meta.com/news/feed/",
    ]),
    "Instagram Blog": (False, [
        "https://about.instagram.com/blog/rss.xml",
        "https://about.instagram.com/blog/rss",
        "https://about.instagram.com/blog/feed",
    ]),
    "TikTok Newsroom": (False, [
        "https://newsroom.tiktok.com/en-us/rss",
        "https://newsroom.tiktok.com/rss",
        "https://newsroom.tiktok.com/feed",
    ]),
    "Google Blog": (False, [
        "https://blog.google/rss/",
        "https://blog.google/rss.xml",
    ]),
    "Google Ads Blog": (False, [
        "https://blog.google/products/ads/rss/",
        "https://blog.google/products/marketingplatform/rss/",
        "https://blog.google/products/ads-commerce/rss/",
    ]),
    "YouTube Blog": (False, [
        "https://blog.youtube/rss/",
        "https://blog.youtube/feeds/posts/default",
    ]),
    "LinkedIn Official Blog": (False, [
        "https://www.linkedin.com/blog/feed",
        "https://www.linkedin.com/blog/member/feed",
        "https://blog.linkedin.com/rss",
    ]),
    "WhatsApp Blog": (False, [
        "https://blog.whatsapp.com/rss",
        "https://blog.whatsapp.com/feed",
        "https://blog.whatsapp.com/rss.xml",
    ]),
    "Social Media Today": (False, [
        "https://www.socialmediatoday.com/feeds/rss/",
        "https://www.socialmediatoday.com/rss.xml",
        "https://www.socialmediatoday.com/rss",
    ]),
    "Social Media Examiner": (False, [
        "https://www.socialmediaexaminer.com/feed/",
    ]),
    "Search Engine Journal": (False, [
        "https://www.searchenginejournal.com/feed/",
    ]),
    "Search Engine Land": (False, [
        "https://searchengineland.com/feed",
        "https://searchengineland.com/feed/",
    ]),
    "Marketing Dive": (False, [
        "https://www.marketingdive.com/feeds/news/",
        "https://www.marketingdive.com/rss/",
    ]),
    "Digiday": (False, [
        "https://digiday.com/feed/",
    ]),
    "Adweek": (False, [
        "https://www.adweek.com/feed/",
        "https://www.adweek.com/feed",
    ]),
    "Marketing Brew": (False, [
        "https://www.marketingbrew.com/feed",
        "https://www.morningbrew.com/marketing/feed",
    ]),
    "Buffer": (False, [
        "https://buffer.com/resources/rss/",
        "https://buffer.com/library/rss/",
        "https://buffer.com/resources/feed/",
    ]),
    "Later": (False, [
        "https://later.com/blog/feed/",
        "https://later.com/feed/",
        "https://later.com/rss.xml",
    ]),
    "Sprout Social": (False, [
        "https://sproutsocial.com/insights/feed/",
        "https://sproutsocial.com/insights/rss/",
    ]),
    "Hootsuite": (False, [
        "https://blog.hootsuite.com/feed/",
        "https://blog.hootsuite.com/rss",
    ]),
    "HubSpot Marketing": (False, [
        "https://blog.hubspot.com/marketing/rss.xml",
    ]),
    "The Verge": (False, [
        "https://www.theverge.com/rss/index.xml",
    ]),
    "TechCrunch": (False, [
        "https://techcrunch.com/feed/",
    ]),
    "Platformer": (False, [
        "https://www.platformer.news/feed",
        "https://www.platformer.news/rss/",
    ]),
    "Ad Age": (False, [
        "https://adage.com/feed",
        "https://adage.com/rss.xml",
    ]),
    "Campaign": (False, [
        "https://www.campaignlive.co.uk/rss",
        "https://www.campaignlive.com/rss",
    ]),
    "TrendHunter": (False, [
        "https://www.trendhunter.com/rss",
        "https://feeds.feedburner.com/trendhunter",
    ]),
    "eMarketer": (False, [
        "https://www.emarketer.com/rss/",
        "https://www.insiderintelligence.com/rss/",
    ]),
    "Think with Google": (False, [
        "https://www.thinkwithgoogle.com/feed/",
        "https://www.thinkwithgoogle.com/intl/en-us/feed/",
    ]),
    "Marketing Week": (False, [
        "https://www.marketingweek.com/feed/",
    ]),
    "ice.co.il": (False, [
        "https://www.ice.co.il/feed",
        "https://www.ice.co.il/rss.xml",
        "https://www.ice.co.il/xml/rss.xml",
    ]),
    "Geektime": (False, [
        "https://www.geektime.co.il/feed/",
    ]),
    "Calcalist": (False, [
        "https://www.calcalist.co.il/GeneralRSS/0,16335,L-3,00.xml",
        "https://www.calcalist.co.il/GeneralRSS/0,16335,L-8,00.xml",
        "https://www.calcalist.co.il/rss/rss.xml",
    ]),
    "Globes": (False, [
        "https://www.globes.co.il/webservice/rss/rssfeeder.asmx/FeederNode?iID=942",
        "https://www.globes.co.il/webservice/rss/rssfeeder.asmx/FeederNode?iID=585",
        "https://www.globes.co.il/webservice/rss/rssfeeder.asmx/FeederNode?iID=2725",
    ]),
    "TheMarker": (False, [
        "https://www.themarker.com/cmlink/1.145",
        "https://www.themarker.com/srv/rss---all-articles",
        "https://www.themarker.com/cmlink/1.1500",
    ]),
    # --- Additional sources suggested by Claude (not in the original list) ---
    "Content Marketing Institute": (True, [
        "https://contentmarketinginstitute.com/feed/",
    ]),
    "MarketingProfs": (True, [
        "https://www.marketingprofs.com/rss/latest.xml",
        "https://www.marketingprofs.com/rss/marketingprofs-daily-rss.xml",
    ]),
    "Moz Blog": (True, [
        "https://moz.com/posts/feed",
    ]),
    "AdExchanger": (True, [
        "https://www.adexchanger.com/feed/",
    ]),
    "MarTech": (True, [
        "https://martech.org/feed/",
    ]),
    "Neil Patel Blog": (True, [
        "https://neilpatel.com/blog/feed/",
    ]),
    "PPC Hero": (True, [
        "https://www.ppchero.com/feed/",
    ]),
    "Search Engine Roundtable": (True, [
        "https://www.seroundtable.com/index.xml",
    ]),
}


def local_name(tag):
    return tag.split("}", 1)[-1] if "}" in tag else tag


def parse_date(text):
    """Try RFC-822 (RSS) then ISO-8601 (Atom) date parsing."""
    text = text.strip()
    if not text:
        return None
    try:
        dt = parsedate_to_datetime(text)
        if dt is not None:
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
    except (TypeError, ValueError, IndexError):
        pass
    iso_text = text.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(iso_text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except ValueError:
        pass
    return None


def find_latest_item_date(root):
    """Return (item_count, latest_date or None)."""
    entries = [el for el in root.iter() if local_name(el.tag).lower() in ("item", "entry")]
    if not entries:
        return 0, None

    dates = []
    for entry in entries:
        found_date = None
        for child in entry.iter():
            lname = local_name(child.tag).lower()
            if lname in ("pubdate", "published", "updated", "date", "issued"):
                candidate = parse_date(child.text or "")
                if candidate:
                    found_date = candidate
                    break
        if found_date:
            dates.append(found_date)

    latest = max(dates) if dates else None
    return len(entries), latest


def looks_like_html(text_sample):
    lowered = text_sample.strip().lower()
    return lowered.startswith("<!doctype html") or bool(re.match(r"^\s*<html[\s>]", lowered))


def check_candidate(url):
    """Return a dict describing the outcome of checking a single candidate URL."""
    result = {"url": url, "ok": False, "reason": None, "item_count": 0, "latest_date": None}
    try:
        resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT, allow_redirects=True)
    except requests.exceptions.SSLError as exc:
        result["reason"] = f"שגיאת SSL: {exc}"
        return result
    except requests.exceptions.Timeout:
        result["reason"] = "פסק זמן (timeout) בחיבור"
        return result
    except requests.exceptions.ConnectionError as exc:
        result["reason"] = f"שגיאת חיבור: {exc}"
        return result
    except requests.exceptions.RequestException as exc:
        result["reason"] = f"שגיאת בקשה: {exc}"
        return result

    result["status_code"] = resp.status_code
    if resp.status_code != 200:
        result["reason"] = f"קוד סטטוס {resp.status_code} (לא 200)"
        return result

    sample = resp.text[:1000]
    if looks_like_html(sample):
        result["reason"] = "התוכן שחזר הוא עמוד HTML, לא XML"
        return result

    try:
        root = ET.fromstring(resp.content)
    except ET.ParseError as exc:
        result["reason"] = f"XML לא תקין: {exc}"
        return result

    root_name = local_name(root.tag).lower()
    if root_name not in ("rss", "feed", "rdf"):
        result["reason"] = f"לא נראה כמו RSS/Atom תקין (root tag: {root.tag})"
        return result

    item_count, latest_date = find_latest_item_date(root)
    result["item_count"] = item_count
    if item_count == 0:
        result["reason"] = "ה-feed תקין אך ריק (0 פריטים)"
        return result

    result["latest_date"] = latest_date.isoformat() if latest_date else None
    if latest_date is None:
        result["reason"] = "לא ניתן היה לזהות תאריך פרסום לפריטים"
        # Still counts as structurally OK, freshness unknown.

    result["ok"] = True
    return result


def verify_source(name, is_extra, candidates):
    attempts = []
    working = None
    for url in candidates:
        r = check_candidate(url)
        attempts.append(r)
        if r["ok"]:
            working = r
            break

    source_result = {
        "name": name,
        "extra": is_extra,
        "candidates_tried": len(attempts),
        "attempts": attempts,
    }

    if working is None:
        source_result["status"] = "failed"
        return source_result

    if working["latest_date"] is None:
        source_result["status"] = "active_unknown_freshness"
        source_result["url"] = working["url"]
        source_result["item_count"] = working["item_count"]
        return source_result

    latest_dt = datetime.fromisoformat(working["latest_date"])
    days_since = (datetime.now(timezone.utc) - latest_dt).days
    source_result["url"] = working["url"]
    source_result["item_count"] = working["item_count"]
    source_result["last_item_date"] = working["latest_date"]
    source_result["days_since_update"] = days_since

    if days_since > DEAD_AFTER_DAYS:
        source_result["status"] = "dead"
    else:
        source_result["status"] = "active"

    return source_result


def main():
    print(f"בודק {len(SOURCES)} מקורות RSS...", file=sys.stderr)
    results = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {
            pool.submit(verify_source, name, is_extra, candidates): name
            for name, (is_extra, candidates) in SOURCES.items()
        }
        for future in as_completed(futures):
            name = futures[future]
            try:
                res = future.result()
            except Exception as exc:  # noqa: BLE001
                res = {"name": name, "status": "failed", "attempts": [], "error": str(exc)}
            results[name] = res
            print(f"  [{res['status']:>22}] {name}", file=sys.stderr)

    # Preserve original ordering from SOURCES dict
    ordered = [results[name] for name in SOURCES]

    active = [r for r in ordered if r["status"] == "active"]
    active_unknown = [r for r in ordered if r["status"] == "active_unknown_freshness"]
    dead = [r for r in ordered if r["status"] == "dead"]
    failed = [r for r in ordered if r["status"] == "failed"]

    write_json(active + active_unknown)
    write_report(ordered, active, active_unknown, dead, failed)

    print(
        f"\nסיכום: {len(active) + len(active_unknown)} עברו, "
        f"{len(dead)} מתים (stale), {len(failed)} נכשלו לגמרי, מתוך {len(ordered)} מקורות.",
        file=sys.stderr,
    )


def write_json(verified_sources):
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dead_after_days": DEAD_AFTER_DAYS,
        "verified_count": len(verified_sources),
        "sources": [],
    }
    for r in verified_sources:
        entry = {
            "name": r["name"],
            "url": r["url"],
            "extra": r["extra"],
            "status": r["status"],
            "item_count": r.get("item_count"),
        }
        if r["status"] == "active":
            entry["last_item_date"] = r["last_item_date"]
            entry["days_since_update"] = r["days_since_update"]
        payload["sources"].append(entry)

    with open("feeds_verified.json", "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def write_report(ordered, active, active_unknown, dead, failed):
    now = datetime.now(timezone.utc)
    lines = []
    lines.append("# דוח אימות מקורות RSS")
    lines.append("")
    lines.append(f"נוצר בתאריך: {now.strftime('%Y-%m-%d %H:%M UTC')}")
    lines.append(f"סה\"כ מקורות שנבדקו: {len(ordered)}")
    lines.append(f"feed נחשב \"מת\" אם הפריט האחרון בו ישן מ-{DEAD_AFTER_DAYS} יום.")
    lines.append("")
    lines.append(
        f"**תוצאה כוללת:** {len(active) + len(active_unknown)} עברו בהצלחה, "
        f"{len(dead)} מתים (feed תקין אך לא מתעדכן), {len(failed)} נכשלו לגמרי."
    )
    lines.append("")

    lines.append("## ✅ מקורות פעילים ותקינים")
    lines.append("")
    if active or active_unknown:
        lines.append("| מקור | כתובת RSS | עדכון אחרון | לפני כמה ימים | תוספת? |")
        lines.append("|---|---|---|---|---|")
        for r in active:
            last = datetime.fromisoformat(r["last_item_date"]).strftime("%Y-%m-%d")
            extra = "כן" if r["extra"] else ""
            lines.append(f"| {r['name']} | {r['url']} | {last} | {r['days_since_update']} | {extra} |")
        for r in active_unknown:
            extra = "כן" if r["extra"] else ""
            lines.append(f"| {r['name']} | {r['url']} | לא זוהה | - | {extra} |")
    else:
        lines.append("_אין מקורות שעברו את כל הבדיקות._")
    lines.append("")

    lines.append("## ⚠️ מקורות \"מתים\" (ה-feed תקין אך לא עודכן מעל 60 יום)")
    lines.append("")
    if dead:
        lines.append("| מקור | כתובת RSS | עדכון אחרון | לפני כמה ימים |")
        lines.append("|---|---|---|---|")
        for r in dead:
            last = datetime.fromisoformat(r["last_item_date"]).strftime("%Y-%m-%d")
            lines.append(f"| {r['name']} | {r['url']} | {last} | {r['days_since_update']} |")
    else:
        lines.append("_אין מקורות במצב הזה._")
    lines.append("")

    lines.append("## ❌ מקורות שנכשלו לגמרי")
    lines.append("")
    if failed:
        for r in failed:
            lines.append(f"### {r['name']}" + (" _(תוספת)_" if r.get("extra") else ""))
            for attempt in r["attempts"]:
                status_bit = f"HTTP {attempt.get('status_code')}" if attempt.get("status_code") else ""
                reason = attempt.get("reason") or "לא ידוע"
                lines.append(f"- `{attempt['url']}` — {reason} {('(' + status_bit + ')') if status_bit else ''}")
            lines.append("")
    else:
        lines.append("_כל המקורות נמצאו במצב כלשהו של feed תקין._")
    lines.append("")

    lines.append("## פירוט מלא לכל המקורות (כולל כתובות שנכשלו לפני שנמצאה כתובת תקינה)")
    lines.append("")
    for r in ordered:
        extra_tag = " _(תוספת)_" if r.get("extra") else ""
        lines.append(f"### {r['name']}{extra_tag} — סטטוס: {status_hebrew(r['status'])}")
        for attempt in r["attempts"]:
            mark = "✅" if attempt["ok"] else "❌"
            detail = attempt.get("reason") or f"{attempt.get('item_count', 0)} פריטים, פריט אחרון: {attempt.get('latest_date')}"
            lines.append(f"- {mark} `{attempt['url']}` — {detail}")
        lines.append("")

    with open("feeds_report.md", "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def status_hebrew(status):
    return {
        "active": "פעיל",
        "active_unknown_freshness": "פעיל (תאריך לא זוהה)",
        "dead": "מת (stale)",
        "failed": "נכשל",
    }.get(status, status)


if __name__ == "__main__":
    start = time.time()
    main()
    print(f"זמן ריצה: {time.time() - start:.1f} שניות", file=sys.stderr)
