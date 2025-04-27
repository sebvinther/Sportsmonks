#!/usr/bin/env python3
# build_fixtures.py

import sqlite3
import requests
import time
from requests.exceptions import HTTPError

# ── Configuration ─────────────────────────────────────────────────────────────
API_TOKEN = "oYeoAVFUTQpu7MfoFqbvyiYfgRRkuBWW0p2atkZnySe4X3xrHkjgGhOvI0pd"
BASE_URL  = "https://api.sportmonks.com/v3/football"
DB_PATH   = "sportmonks.db"

# ── Helper: Paginated fetch ───────────────────────────────────────────────────
def fetch_all(endpoint, params=None):
    items, page = [], 1
    while True:
        p = {"api_token": API_TOKEN, "per_page": 1000, "page": page}
        if params: p.update(params)
        resp = requests.get(f"{BASE_URL}/{endpoint}", params=p, timeout=10)
        if resp.status_code == 429:
            wait = resp.json().get("rate_limit", {}).get("resets_in_seconds", 60)
            print(f"Rate-limit hit, sleeping {wait+1}s…")
            time.sleep(wait + 1)
            continue
        resp.raise_for_status()
        data = resp.json().get("data", [])
        if not data:
            break
        items.extend(data)
        if not resp.json().get("pagination", {}).get("has_more", False):
            break
        page += 1
    return items

def main():
    # ── 1. Load & (re)create leagues table ─────────────────────────────────────
    print("Loading leagues…")
    leagues = fetch_all("leagues")
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS leagues;")
        cur.execute("""
            CREATE TABLE leagues (
                id         INTEGER PRIMARY KEY,
                name       TEXT,
                country_id INTEGER,
                logo_path  TEXT,
                slug       TEXT,
                type       TEXT
            );
        """)
        for l in leagues:
            cur.execute("""
                INSERT OR REPLACE INTO leagues
                  (id,name,country_id,logo_path,slug,type)
                VALUES (?,?,?,?,?,?);
            """, (
                l["id"], l["name"], l.get("country_id"),
                l.get("logo_path"), l.get("slug"), l.get("type")
            ))
        conn.commit()
    print(f"✔ Loaded {len(leagues)} leagues")

    # ── 2. Recreate fixtures table ──────────────────────────────────────────────
    print("Rebuilding fixtures table…")
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS fixtures;")
        cur.execute("""
            CREATE TABLE fixtures (
                id            INTEGER PRIMARY KEY,
                league_id     INTEGER,
                date_time     TEXT,
                status        TEXT,
                home_team_id  INTEGER,
                away_team_id  INTEGER,
                home_score    INTEGER,
                away_score    INTEGER
            );
        """)
        conn.commit()

    # ── 3. Fetch fixtures by league using static filter ────────────────────────
    total_fixtures = 0
    with sqlite3.connect(DB_PATH) as conn:
        league_ids = [row[0] for row in conn.execute("SELECT id FROM leagues;")]

    for lid in league_ids:
        print(f"Fetching fixtures for league {lid}…")
        page = 1
        while True:
            params = {
                "api_token": API_TOKEN,
                "filters":   f"fixtureLeagues:{lid}",
                "per_page":  1000,
                "page":      page
            }
            resp = requests.get(f"{BASE_URL}/fixtures", params=params, timeout=10)
            if resp.status_code == 429:
                wait = resp.json().get("rate_limit", {}).get("resets_in_seconds", 60)
                print(f"Rate-limit, sleeping {wait+1}s…")
                time.sleep(wait + 1)
                continue
            resp.raise_for_status()
            data = resp.json().get("data", [])
            if not data:
                break

            with sqlite3.connect(DB_PATH) as conn:
                cur = conn.cursor()
                for f in data:
                    cur.execute("""
                        INSERT OR REPLACE INTO fixtures
                          (id,league_id,date_time,status,
                           home_team_id,away_team_id,home_score,away_score)
                        VALUES (?,?,?,?,?,?,?,?);
                    """, (
                        f["id"], lid,
                        f.get("starting_at"),
                        f.get("time", {}).get("status"),
                        f.get("localteam_id"),
                        f.get("visitorteam_id"),
                        f.get("scores", {}).get("localteam_score"),
                        f.get("scores", {}).get("visitorteam_score")
                    ))
                    total_fixtures += 1
                conn.commit()

            print(f"League {lid}, page {page}: {len(data)} fixtures")
            page += 1
            time.sleep(0.1)

    print(f"✔ Loaded {total_fixtures} total fixtures across {len(leagues)} leagues")

if __name__ == "__main__":
    main()
