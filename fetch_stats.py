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
        resp = requests.get(sched_url, timeout=10)
        print(f"[Live]   Schedule HTTP {resp.status_code}")
        sched = resp.json()
    except Exception as e:
        print(f"WARNING [Live] Schedule fetch failed: {e}")
        return []

    game_pks = []
    for date_block in sched.get("dates", []):
        for game in date_block.get("games", []):
            gtype  = game.get("gameType", "")
            gpk    = game["gamePk"]
            status = game.get("status", {}).get("detailedState", "?")
            print(f"[Live]   Game {gpk}  type={gtype}  status={status}")
            if gtype == "R":
                game_pks.append(gpk)

    if not game_pks:
        print("[Live]   No regular season games found today.")
        return []

    print(f"[Live]   Processing {len(game_pks)} game(s): {game_pks}")

    results = []

    for pk in game_pks:
        feed_url = f"https://statsapi.mlb.com/api/v1.1/game/{pk}/feed/live"
        try:
            feed = requests.get(feed_url, timeout=15).json()
        except Exception as e:
            print(f"WARNING [Live] Feed fetch failed for {pk}: {e}")
            continue

        game_data = feed.get("gameData", {})
        plays     = feed.get("liveData", {}).get("plays", {}).get("allPlays", [])
        home_team = game_data.get("teams", {}).get("home", {}).get("abbreviation", "MLB")
        away_team = game_data.get("teams", {}).get("away", {}).get("abbreviation", "Opp")
        game_date = game_data.get("datetime", {}).get("officialDate") or today

        print(f"[Live]   Game {pk} ({away_team} @ {home_team}) — {len(plays)} total plays")

        for play in plays:
            result     = play.get("result", {})
            event_type = result.get("eventType", "").lower()

            if event_type != "home_run":
                continue

            batter_name = (
                play.get("matchup", {})
                    .get("batter", {})
                    .get("fullName", "Unknown Slugger")
            )

            hit_data = play.get("hitData", {})

            if not hit_data:
                play_events = play.get("playEvents", [])
                if play_events:
                    hit_data = play_events[-1].get("hitData", {})

            dist_raw = (
                hit_data.get("distance")
                or hit_data.get("totalDistance")
                or hit_data.get("hitDistance")
                or hit_data.get("calculatedDistance")
            )

            print(f"[Live]   HR found: {batter_name}  dist={dist_raw}  hitData={hit_data}")

            if dist_raw is None:
                print(f"[Live]   -> Distance not populated yet, skipping.")
                continue

            try:
                dist = float(dist_raw)
            except (ValueError, TypeError):
                continue

            if dist < min_distance:
                print(f"[Live]   -> {dist}ft is under {min_distance}ft threshold, skipping.")
                continue

            inning = play.get("about", {}).get("inning", 0)
            is_top = play.get("about", {}).get("isTopInning", True)

            batting_team  = away_team if is_top else home_team
            fielding_team = home_team if is_top else away_team

            exit_velo    = hit_data.get("launchSpeed")
            launch_angle = hit_data.get("launchAngle")

            results.append({
                "player":       batter_name,
                "distance":     int(dist),
                "team":         batting_team,
                "opponent":     fielding_team,
                "date":         game_date,
                "game_pk":      pk,
                "inning":       inning,
                "exit_velo":    str(exit_velo) if exit_velo is not None else "",
                "launch_angle": str(launch_angle) if launch_angle is not None else "",
                "source":       "live",
            })
            print(f"[Live]   -> ADDED {batter_name} {int(dist)}ft")

    seen = {}
    for h in results:
        key = (h["player"], h["date"])
        if key not in seen or h["distance"] > seen[key]["distance"]:
            seen[key] = h

    print(f"[Live]   Found {len(seen)} qualifying homers (live).")
    return list(seen.values())


# ---------------------------------------------------------------------------
# 3. MAIN — merge both, sort, save
# ---------------------------------------------------------------------------
def get_season_baja_bombs(min_distance=MIN_DISTANCE):
    historical = get_savant_bombs(min_distance)
    live       = get_live_bombs(min_distance)

    combined = {}
    for h in historical:
        combined[(h["player"], h["date"])] = h
    for h in live:
        key = (h["player"], h["date"])
        if key not in combined or h["distance"] > combined[key]["distance"]:
            combined[key] = h

    sorted_list = sorted(
        combined.values(),
        key=lambda x: (x["date"], x["game_pk"], x["inning"])
    )

    output_file = "data.json"
    if not sorted_list:
        print("WARNING No results from either source — data.json left unchanged.")
    else:
        with open(output_file, "w") as f:
            json.dump(sorted_list, f, indent=4)
        print(f"\nTotal: {len(sorted_list)} Baja Blasts (>={min_distance}ft) saved to {output_file}.")
        for bomb in sorted_list:
            tag = "LIVE" if bomb.get("source") == "live" else "    "
            print(f"  [{tag}] {bomb['date']}  Inn {bomb['inning']}  {bomb['player']:25s}  {bomb['distance']}ft  EV:{bomb['exit_velo'] or '?'} LA:{bomb['launch_angle'] or '?'}")


if __name__ == "__main__":
    get_season_baja_bombs()
