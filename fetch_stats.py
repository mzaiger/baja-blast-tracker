import requests
import csv
import json
import io
from datetime import datetime, timedelta, timezone

MIN_DISTANCE = 420
START_DATE   = "2026-03-20"

# ---------------------------------------------------------------------------
# 1. HISTORICAL — Baseball Savant CSV (season start → yesterday)
# ---------------------------------------------------------------------------
def get_savant_bombs(min_distance=MIN_DISTANCE):
    today     = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime('%Y-%m-%d')

    url = (
        "https://baseballsavant.mlb.com/statcast_search/csv"
        "?all=true"
        "&hfAB=home_run%7C"
        "&hfGT=R%7C"
        f"&game_date_gt={START_DATE}"
        f"&game_date_lt={today}"
        "&hfSea=2026%7C"
        "&type=details"
        "&player_type=batter"
    )

    headers = {"User-Agent": "Mozilla/5.0 (compatible; StatcastFetcher/1.0)"}

    print(f"[Savant] Fetching historical data ({START_DATE} -> {yesterday})...")

    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()

    content = response.text
    if not content.strip():
        print("WARNING [Savant] Empty response -- skipping historical fetch.")
        return []

    reader  = csv.DictReader(io.StringIO(content))
    results = []

    for row in reader:
        dist_str = row.get("hit_distance_sc", "").strip()
        if not dist_str:
            continue
        try:
            dist = float(dist_str)
        except ValueError:
            continue

        if dist < min_distance:
            continue

        game_date = row.get("game_date", "")
        if game_date == today:
            continue

        results.append({
            "player":       row.get("player_name", "Unknown Slugger"),
            "distance":     int(dist),
            "team":         row.get("home_team", "MLB"),
            "opponent":     row.get("away_team", "Opp"),
            "date":         game_date,
            "game_pk":      int(row.get("game_pk", 0) or 0),
            "inning":       int(row.get("inning", 0) or 0),
            "exit_velo":    row.get("launch_speed", ""),
            "launch_angle": row.get("launch_angle", ""),
            "source":       "savant",
        })

    seen = {}
    for h in results:
        key = (h["player"], h["date"])
        if key not in seen or h["distance"] > seen[key]["distance"]:
            seen[key] = h

    print(f"[Savant] Found {len(seen)} qualifying homers (historical).")
    return list(seen.values())


# ---------------------------------------------------------------------------
# 2. LIVE — MLB Stats API play-by-play (today only)
# ---------------------------------------------------------------------------
def get_live_bombs(min_distance=MIN_DISTANCE):
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')

    sched_url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={today}"

    print(f"[Live]   Fetching schedule for {today}...")

    try:
        resp = requests.get