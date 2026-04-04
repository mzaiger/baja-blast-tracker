import requests
import csv
import json
import io
from datetime import datetime

def get_season_baja_bombs(min_distance=420):
    start_date = "2026-03-20"
    end_date = datetime.now().strftime('%Y-%m-%d')

    # Baseball Savant CSV export — the correct source for Statcast hit distance
    url = (
        "https://baseballsavant.mlb.com/statcast_search/csv"
        "?all=true"
        "&hfAB=home_run%7C"   # home runs only
        "&hfGT=R%7C"          # regular season only
        f"&game_date_gt={start_date}"
        f"&game_date_lt={end_date}"
        "&hfSea=2026%7C"
        "&type=details"
        "&player_type=batter"
    )

    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; StatcastFetcher/1.0)"
    }

    print(f"Fetching Statcast data from Baseball Savant ({start_date} → {end_date})...")

    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()  # raises immediately on HTTP errors — no silent failures

    content = response.text
    if not content.strip():
        raise ValueError("Baseball Savant returned an empty response.")

    reader = csv.DictReader(io.StringIO(content))

    baja_list = []
    for row in reader:
        dist_str = row.get("hit_distance_sc", "").strip()
        if not dist_str:
            continue
        try:
            dist = float(dist_str)
        except ValueError:
            continue

        if dist >= min_distance:
            baja_list.append({
                "player":       row.get("player_name", "Unknown Slugger"),
                "distance":     int(dist),
                "team":         row.get("home_team", "MLB"),
                "opponent":     row.get("away_team", "Opp"),
                "date":         row.get("game_date", ""),
                "game_pk":      int(row.get("game_pk", 0) or 0),  # unique game ID — cast to int for correct sort
                "inning":       int(row.get("inning", 0) or 0),
                "exit_velo":    row.get("launch_speed", ""),
                "launch_angle": row.get("launch_angle", ""),
            })

    # Deduplicate by (player, date) — Savant can return one row per pitch/play
    seen = {}
    for h in baja_list:
        key = (h["player"], h["date"])
        # keep the longer bomb if there's a collision
        if key not in seen or h["distance"] > seen[key]["distance"]:
            seen[key] = h

    # Sort by date then game_pk (proxy for game time) then inning for full chronological order
    sorted_list = sorted(
        seen.values(),
        key=lambda x: (x["date"], x["game_pk"], x["inning"])
    )

    output_file = "data.json"
    with open(output_file, "w") as f:
        json.dump(sorted_list, f, indent=4)

    print(f"✅ Found {len(sorted_list)} Baja Blasts (≥{min_distance}ft). Saved to {output_file}.")
    for bomb in sorted_list:
        print(f"  💣 {bomb['date']}  Inning {bomb['inning']}  {bomb['player']:25s} {bomb['distance']}ft")

if __name__ == "__main__":
    get_season_baja_bombs()
