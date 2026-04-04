import requests
import csv
import json
import io
from datetime import datetime, timedelta

MIN_DISTANCE = 420
START_DATE   = "2026-03-20"


# ---------------------------------------------------------------------------
# 1. HISTORICAL — Baseball Savant CSV (season start → yesterday)
#    Full Statcast data, updated overnight. Reliable distance / exit velo.
# ---------------------------------------------------------------------------
def get_savant_bombs(min_distance=MIN_DISTANCE):
    today     = datetime.now().strftime('%Y-%m-%d')
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    url = (
        "https://baseballsavant.mlb.com/statcast_search/csv"
        "?all=true"
        "&hfAB=home_run%7C"        # home runs only
        "&hfGT=R%7C"               # regular season only
        f"&game_date_gt={START_DATE}"
        f"&game_date_lt={today}"   # Savant is next-morning, so today filter is safe
        "&hfSea=2026%7C"
        "&type=details"
        "&player_type=batter"
    )

    headers = {"User-Agent": "Mozilla/5.0 (compatible; StatcastFetcher/1.0)"}

    print(f"[Savant] Fetching historical data ({START_DATE} → {yesterday})...")

    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()

    content = response.text
    if not content.strip():
        print("⚠️  [Savant] Empty response — skipping historical fetch.")
        return []

    reader   = csv.DictReader(io.StringIO(content))
    results  = []

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

        # Skip today — those will come from the live feed
        game_date = row.get("game_date", "")
        if game_date == datetime.now().strftime('%Y-%m-%d'):
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

    # Deduplicate by (player, date) — keep longest
    seen = {}
    for h in results:
        key = (h["player"], h["date"])
        if key not in seen or h["distance"] > seen[key]["distance"]:
            seen[key] = h

    print(f"[Savant] ✅ {len(seen)} qualifying homers found (historical).")
    return list(seen.values())


# ---------------------------------------------------------------------------
# 2. LIVE — MLB Stats API play-by-play (today only)
#    Updates pitch-by-pitch during games. Statcast fields populate quickly
#    but may still be blank for plays just seconds old — that's normal.
# ---------------------------------------------------------------------------
def get_live_bombs(min_distance=MIN_DISTANCE):
    today = datetime.now().strftime('%Y-%m-%d')

    # Step 1: get all of today's game PKs
    sched_url = (
        f"https://statsapi.mlb.com/api/v1/schedule"
        f"?sportId=1&date={today}&gameType=R"
    )

    print(f"[Live]   Fetching today's schedule ({today})...")

    try:
        sched = requests.get(sched_url, timeout=10).json()
    except Exception as e:
        print(f"⚠️  [Live] Schedule fetch failed: {e}")
        return []

    game_pks = []
    for date_block in sched.get("dates", []):
        for game in date_block.get("games", []):
            game_pks.append(game["gamePk"])

    if not game_pks:
        print("[Live]   No games today.")
        return []

    print(f"[Live]   Found {len(game_pks)} game(s): {game_pks}")

    results = []

    # Step 2: pull live play-by-play for each game
    for pk in game_pks:
        feed_url = f"https://statsapi.mlb.com/api/v1.1/game/{pk}/feed/live"
        try:
            feed = requests.get(feed_url, timeout=15).json()
        except Exception as e:
            print(f"⚠️  [Live] Feed fetch failed for game {pk}: {e}")
            continue

        game_data  = feed.get("gameData", {})
        live_data  = feed.get("liveData", {})
        plays      = live_data.get("plays", {}).get("allPlays", [])

        home_team  = game_data.get("teams", {}).get("home", {}).get("abbreviation", "MLB")
        away_team  = game_data.get("teams", {}).get("away", {}).get("abbreviation", "Opp")
        game_date  = game_data.get("datetime", {}).get("officialDate", today)

        for play in plays:
            result = play.get("result", {})

            # Only home runs
            if result.get("eventType") != "home_run":
                continue

            hit_data = play.get("hitData", {})
            dist_raw = hit_data.get("totalDistance")
            if dist_raw is None:
                continue

            try:
                dist = float(dist_raw)
            except (ValueError, TypeError):
                continue

            if dist < min_distance:
                continue

            batter_name = (
                play.get("matchup", {})
                    .get("batter", {})
                    .get("fullName", "Unknown Slugger")
            )
            inning = play.get("about", {}).get("inning", 0)

            exit_velo    = hit_data.get("launchSpeed")
            launch_angle = hit_data.get("launchAngle")

            results.append({
                "player":       batter_name,
                "distance":     int(dist),
                "team":         home_team,
                "opponent":     away_team,
                "date":         game_date,
                "game_pk":      pk,
                "inning":       inning,
                "exit_velo":    str(exit_velo) if exit_velo is not None else "",
                "launch_angle": str(launch_angle) if launch_angle is not None else "",
                "source":       "live",
            })

    # Deduplicate by (player, date) — keep longest
    seen = {}
    for h in results:
        key = (h["player"], h["date"])
        if key not in seen or h["distance"] > seen[key]["distance"]:
            seen[key] = h

    print(f"[Live]   ✅ {len(seen)} qualifying homers found (today, live).")
    return list(seen.values())


# ---------------------------------------------------------------------------
# 3. MAIN — merge both sources, sort, save
# ---------------------------------------------------------------------------
def get_season_baja_bombs(min_distance=MIN_DISTANCE):
    historical = get_savant_bombs(min_distance)
    live       = get_live_bombs(min_distance)

    # Merge — live takes priority for today (deduplicate by player+date)
    combined = {}
    for h in historical:
        combined[(h["player"], h["date"])] = h
    for h in live:
        key = (h["player"], h["date"])
        # Live entry wins if longer, or if no historical entry exists
        if key not in combined or h["distance"] > combined[key]["distance"]:
            combined[key] = h

    sorted_list = sorted(
        combined.values(),
        key=lambda x: (x["date"], x["game_pk"], x["inning"])
    )

    output_file = "data.json"
    if not sorted_list:
        print("⚠️  No results from either source — data.json left unchanged.")
    else:
        with open(output_file, "w") as f:
            json.dump(sorted_list, f, indent=4)
        print(f"\n✅ Total: {len(sorted_list)} Baja Blasts (≥{min_distance}ft) saved to {output_file}.")
        for bomb in sorted_list:
            tag = "🔴 LIVE" if bomb.get("source") == "live" else "    "
            print(f"  {tag} {bomb['date']}  Inn {bomb['inning']}  {bomb['player']:25s}  {bomb['distance']}ft  EV:{bomb['exit_velo'] or '?'} LA:{bomb['launch_angle'] or '?'}")


if __name__ == "__main__":
    get_season_baja_bombs()
