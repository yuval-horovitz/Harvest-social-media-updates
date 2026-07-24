#!/usr/bin/env python3
"""
Collection engine: pull items from the verified RSS/Atom feeds and store them
in a local SQLite database, deduplicated by link.

On the very first run (empty database), items published in the last
COLLECT_WINDOW_DAYS days are collected. On every later run, all items
returned by each feed are considered, but only ones not already in the
database (by link hash) are inserted - this is what "collect only new items"
means in practice, since feeds only ever expose their most recent items.

This script does not analyze, filter, or judge content - it only harvests
and stores it as-is. Analysis is a later stage of the project.
"""

import hashlib
import json
import logging
import sqlite3
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

import requests

FEEDS_FILE = "feeds_verified.json"
DB_FILE = "harvest.db"
COLLECT_WINDOW_DAYS = 7
REQUEST_TIMEOUT = (10, 20)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/rss+xml, application/atom+xml, application/xml;q=0.9, text/xml;q=0.8, */*;q=0.5",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("collect.log"), logging.StreamHandler(sys.stderr)],
)
log = logging.getLogger("collect")


def local_name(tag):
    return tag.split("}", 1)[-1] if "}" in tag else tag


def parse_date(text):
    text = (text or "").strip()
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
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except ValueError:
        pass
    return None


def extract_link(entry):
    for child in entry:
        if local_name(child.tag).lower() != "link":
            continue
        href = child.attrib.get("href")
        if href:
            return href.strip()
        if child.text and child.text.strip():
            return child.text.strip()
    return None


def extract_text(entry, names):
    for child in entry:
        if local_name(child.tag).lower() in names:
            if child.text and child.text.strip():
                return child.text.strip()
    return None


def extract_date(entry):
    for child in entry:
        if local_name(child.tag).lower() in ("pubdate", "published", "updated", "date", "issued"):
            dt = parse_date(child.text)
            if dt:
                return dt
    return None


def fetch_feed_items(url):
    """Return a list of raw item dicts: title, link, published_at (datetime|None), summary."""
    resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT, allow_redirects=True)
    resp.raise_for_status()
    root = ET.fromstring(resp.content)

    entries = [el for el in root.iter() if local_name(el.tag).lower() in ("item", "entry")]
    items = []
    for entry in entries:
        link = extract_link(entry)
        if not link:
            continue
        title = extract_text(entry, ("title",)) or "(ללא כותרת)"
        summary = extract_text(entry, ("description", "summary", "content", "encoded"))
        published_at = extract_date(entry)
        items.append({"title": title, "link": link, "published_at": published_at, "summary": summary})
    return items


def item_id(link):
    return hashlib.sha256(link.strip().encode("utf-8")).hexdigest()


def init_db(conn):
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS items (
            id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            link TEXT NOT NULL,
            published_at TEXT,
            source_name TEXT NOT NULL,
            summary TEXT,
            collected_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            sources_total INTEGER,
            sources_ok INTEGER,
            sources_failed INTEGER,
            items_fetched INTEGER,
            items_new INTEGER,
            items_duplicate INTEGER
        );

        CREATE TABLE IF NOT EXISTS run_source_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL REFERENCES runs(id),
            source_name TEXT NOT NULL,
            status TEXT NOT NULL,
            items_fetched INTEGER DEFAULT 0,
            items_new INTEGER DEFAULT 0,
            items_duplicate INTEGER DEFAULT 0,
            error_message TEXT
        );

        CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value TEXT
        );
        """
    )
    conn.commit()


def get_meta(conn, key):
    row = conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
    return row[0] if row else None


def set_meta(conn, key, value):
    conn.execute(
        "INSERT INTO meta (key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (key, value),
    )


def load_sources():
    with open(FEEDS_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    sources = [s for s in data["sources"] if s.get("status") == "active" and s["name"] != "TrendHunter"]
    return sources


def collect(conn, sources, is_first_run):
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=COLLECT_WINDOW_DAYS) if is_first_run else None

    run_started_at = now.isoformat()
    cur = conn.execute(
        "INSERT INTO runs (started_at, sources_total) VALUES (?, ?)",
        (run_started_at, len(sources)),
    )
    run_id = cur.lastrowid
    conn.commit()

    per_source_stats = []
    sources_ok = 0
    sources_failed = 0
    total_fetched = 0
    total_new = 0
    total_duplicate = 0

    for source in sources:
        name = source["name"]
        url = source["url"]
        try:
            raw_items = fetch_feed_items(url)
        except Exception as exc:  # noqa: BLE001 - a bad feed must never stop the run
            sources_failed += 1
            log.error("נכשל בשליפת %s (%s): %s", name, url, exc)
            conn.execute(
                "INSERT INTO run_source_log (run_id, source_name, status, error_message) "
                "VALUES (?, ?, 'error', ?)",
                (run_id, name, str(exc)),
            )
            per_source_stats.append({"name": name, "status": "error", "fetched": 0, "new": 0, "duplicate": 0, "error": str(exc)})
            continue

        fetched = len(raw_items)
        new_count = 0
        duplicate_count = 0

        for raw in raw_items:
            if cutoff is not None:
                if raw["published_at"] is not None and raw["published_at"] < cutoff:
                    continue

            rid = item_id(raw["link"])
            exists = conn.execute("SELECT 1 FROM items WHERE id = ?", (rid,)).fetchone()
            if exists:
                duplicate_count += 1
                continue

            conn.execute(
                "INSERT INTO items (id, title, link, published_at, source_name, summary, collected_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    rid,
                    raw["title"],
                    raw["link"],
                    raw["published_at"].isoformat() if raw["published_at"] else None,
                    name,
                    raw["summary"],
                    now.isoformat(),
                ),
            )
            new_count += 1

        conn.execute(
            "INSERT INTO run_source_log (run_id, source_name, status, items_fetched, items_new, items_duplicate) "
            "VALUES (?, ?, 'ok', ?, ?, ?)",
            (run_id, name, fetched, new_count, duplicate_count),
        )
        conn.commit()

        sources_ok += 1
        total_fetched += fetched
        total_new += new_count
        total_duplicate += duplicate_count
        per_source_stats.append(
            {"name": name, "status": "ok", "fetched": fetched, "new": new_count, "duplicate": duplicate_count}
        )
        log.info("%s: %d פריטים נמצאו, %d חדשים, %d כפילויות", name, fetched, new_count, duplicate_count)

    finished_at = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "UPDATE runs SET finished_at = ?, sources_ok = ?, sources_failed = ?, "
        "items_fetched = ?, items_new = ?, items_duplicate = ? WHERE id = ?",
        (finished_at, sources_ok, sources_failed, total_fetched, total_new, total_duplicate, run_id),
    )
    set_meta(conn, "last_run_finished_at", finished_at)
    conn.commit()

    return {
        "run_id": run_id,
        "sources_ok": sources_ok,
        "sources_failed": sources_failed,
        "items_fetched": total_fetched,
        "items_new": total_new,
        "items_duplicate": total_duplicate,
        "per_source": per_source_stats,
    }


def print_summary(summary, is_first_run):
    print()
    print("=" * 60)
    print(f"סיכום ריצת איסוף ({'ריצה ראשונה - חלון 7 ימים' if is_first_run else 'ריצה רגילה - רק פריטים חדשים'})")
    print("=" * 60)
    for s in summary["per_source"]:
        if s["status"] == "ok":
            print(f"  {s['name']:<28} נמצאו: {s['fetched']:>4}  חדשים: {s['new']:>4}  כפילויות: {s['duplicate']:>4}")
        else:
            print(f"  {s['name']:<28} נכשל: {s['error']}")
    print("-" * 60)
    print(f"מקורות שהצליחו: {summary['sources_ok']} / נכשלו: {summary['sources_failed']}")
    print(f"סה\"כ פריטים שנמצאו בפועל בפידים: {summary['items_fetched']}")
    print(f"סה\"כ פריטים חדשים שנשמרו: {summary['items_new']}")
    print(f"סה\"כ כפילויות שנמנעו: {summary['items_duplicate']}")
    print("=" * 60)


def main():
    sources = load_sources()
    conn = sqlite3.connect(DB_FILE)
    init_db(conn)

    is_first_run = get_meta(conn, "last_run_finished_at") is None
    log.info("מתחיל ריצת איסוף על %d מקורות (%s)", len(sources), "ריצה ראשונה" if is_first_run else "ריצה רגילה")

    summary = collect(conn, sources, is_first_run)
    conn.close()

    print_summary(summary, is_first_run)


if __name__ == "__main__":
    main()
