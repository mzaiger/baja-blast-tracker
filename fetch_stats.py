import requests
import csv
import json
import io
from datetime import datetime, timedelta, timezone

MIN_DISTANCE = 420
START_DATE   = "2026-03-20"

# Savant refreshes around 4 AM ET each morning. We keep yesterday's games
# in the "live" (MLB Stats API) bucket until 08:00 UTC the following day
# to ensure Savant has had time to populate before we switch sources.
SAVANT_READY_HOUR_UTC = 15  # matches first cron run (10am CDT); live feed covers until then


def get_live_date():
    """Return the calendar date whose games are still treated as 'live'.

    Before 08:00 UTC  → yesterday (Savant hasn't refreshed yet)
    After  08:00 UTC  → today     (Savant is current through yesterday)
    """
    now     = datetime.now(timezone.utc)
    cutoff  = now.replace(hour=SAVANT_READY_HOUR_UTC, minute=0, second=0, microsecond=0)
    if now < cutoff:
        return (now - timedelta(days=1)).strftime('%Y-%m-%d')
    return now.strftime('%Y-%m-%d')


def get_game_start_times(game_pks):
    """Return {game_pk: iso_datetime_str} from the MLB schedule API.
    Batches lookups by fetching one schedule call per unique date."""
    if not game_pks:
        return {}

    # Fetch each game individually via gamePk — most reliable
    start_times = {}
    for pk in set(game_pks):
        try:
            url  = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&gamePk={pk}"
            data = requests.get(url, timeout=10).json()
            for date_block in data.get("dates", []):
                for game in date_block.get("games", []):
                    if game.get("gamePk") == pk:
                        dt = game.get("gameDate", "")  # ISO UTC string
                        if dt:
                            start_times[pk] = dt
        except Exception as e:
            print(f"WARNING [Schedule] Could not fetch start time for {pk}: {e}")

    return start_times



def get_savant_bombs(min_distance=MIN_DISTANCE, live_date=None):
    if live_date is None:
        live_date = get_live_date()

    # Fetch everything strictly before live_date so there's no overlap with
    # the live feed (game_date_lt is exclusive on the Savant side).
    url = (
        "https://baseballsavant.mlb.com/statcast_search/csv"
        "?all=true"
        "&hfAB=home_run%7C"
        "&hfGT=R%7C"
        f"&game_date_gt={START_DATE}"
        f"&game_date_lt={live_date}"
        "&hfSea=2026%7C"
        "&type=details"
        "&player_type=batter"
    )

    headers = {"User-Agent": "Mozilla/5.0 (compatible; StatcastFetcher/1.0)"}

    savant_thru = (
        datetime.strptime(live_date, "%Y-%m-%d") - timedelta(days=1)
    ).strftime("%Y-%m-%d")
    print(f"[Savant] Fetching historical data ({START_DATE} -> {savant_thru})...")

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
        # Safety net: drop anything on or after live_date even if Savant
        # somehow returned it (e.g. the query boundary is inclusive).
        if game_date >= live_date:
            continue

        results.append({
            "player":       row.get("player_name", "Unknown Slugger"),
            "distance":     int(dist),
            "team":         row.get("home_team", "MLB"),
            "opponent":     row.get("away_team", "Opp"),
            "date":         game_date,
            "time_utc":     "",  # filled in below after batch lookup
            "game_pk":      int(row.get("game_pk", 0) or 0),
            "inning":       int(row.get("inning", 0) or 0),
            "exit_velo":    row.get("launch_speed", ""),
            "launch_angle": row.get("launch_angle", ""),
            "source":       "savant",
        })

    # Batch-fetch game start times so we can sort within a day
    unique_pks = {h["game_pk"] for h in results if h["game_pk"]}
    print(f"[Savant] Fetching start times for {len(unique_pks)} unique game(s)...")
    start_times = get_game_start_times(unique_pks)
    for h in results:
        h["time_utc"] = start_times.get(h["game_pk"], "")

    seen = {}
    for h in results:
        key = (h["player"], h["date"])
        if key not in seen or h["distance"] > seen[key]["distance"]:
            seen[key] = h

    print(f"[Savant] Found {len(seen)} qualifying homers (historical).")
    return list(seen.values())


# ---------------------------------------------------------------------------
# 2. LIVE — MLB Stats API play-by-play (live_date only)
# ---------------------------------------------------------------------------
def get_live_bombs(min_distance=MIN_DISTANCE, live_date=None):
    if live_date is None:
        live_date = get_live_date()

    sched_url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={live_date}"

    print(f"[Live]   Fetching schedule for {live_date}...")

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
        game_date = game_data.get("datetime", {}).get("officialDate") or live_date

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
            time_utc = play.get("about", {}).get("startTime", "")

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
                "time_utc":     time_utc,
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
    live_date = get_live_date()
    savant_thru = (datetime.strptime(live_date, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
    print(f"[Main]   live_date={live_date}  (Savant covers up to {savant_thru}, live feed covers {live_date})")

    historical = get_savant_bombs(min_distance, live_date=live_date)
    live       = get_live_bombs(min_distance, live_date=live_date)

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
